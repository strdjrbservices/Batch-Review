from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from cred import *
from dataset_writer import save_validation_sample, save_analysis_sample
import time
import os
from datetime import datetime
import logging
import concurrent.futures
import threading
import shutil
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Suppress webdriver_manager logs
os.environ['WDM_LOG'] = '0'

def setup_logging():
    """Configures logging to a file and the console."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    # Create a timestamped log file
    log_filename = os.path.join(log_dir, f"batch_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # Set the lowest level for the logger

    # Remove any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create a console handler to display logs in the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Suppress overly verbose logs from third-party libraries
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("webdriver_manager").setLevel(logging.WARNING)
 
# Global lock for report writing to prevent race conditions
report_lock = threading.Lock()

def create_log_report(log_data, start_time, end_time, pdf_filename, report_path="reports\review_log.txt", retries=0):
    """
    Creates a text file log report from the captured data.
 
    Args:
        log_data (dict): A dictionary containing the log data.
        start_time (str): The formatted start time of the review.
        end_time (str): The formatted end time of the review.
        pdf_filename (str): The name of the PDF file that was reviewed.
        report_path (str): The full path to save the log file.
        retries (int): The number of retries attempted during the process.
    """
    try:
        # NOTE: Changed report_path to use os.path.join for cross-platform compatibility
        report_path = report_path.replace("\\", os.sep)
        
        # Ensure the directory exists
        report_dir = os.path.dirname(report_path)
        if report_dir:
            os.makedirs(report_dir, exist_ok=True)
 
        with report_lock:
            with open(report_path, 'a', encoding='utf-8') as f:
                f.write("\n" + "="*80 + "\n\n")
                f.write("Appraisal Review Log Report\n")
                f.write("="*30 + "\n\n")
                f.write(f"File Name: {os.path.basename(pdf_filename)}\n")
                f.write(f"Start Time: {start_time}\n")
                f.write(f"End Time: {end_time}\n")
                f.write(f"Retries: {retries}\n\n")
                for section, messages in log_data.items():
                    f.write(f"--- {section} ---\n")
                    if messages:
                        for entry in messages:
                            if isinstance(entry, tuple):
                                f.write(f"[{entry[0]}] - {entry[1]}\n")
                            else:
                                f.write(f"- {entry}\n")
                    else:
                        f.write("- No validation messages captured.\n")
                    f.write("\n")
        logging.debug(f"Log report appended successfully to: {report_path}")
    except Exception as e:
        logging.error(f"Error creating log report: {e}")
 
def get_processed_files(report_path):
    """
    Reads the log report to identify files that have already been processed.
    """
    processed_files = set()
    if os.path.exists(report_path):
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip().startswith("File Name: "):
                        filename = line.strip().split("File Name: ")[1].strip()
                        processed_files.add(filename)
        except Exception as e:
            logging.error(f"Error reading report file: {e}")
    return processed_files

def send_email_notification(subject, body, attachment_path=None, 
                            sender=None, receiver=None, cc=None, password=None, 
                            smtp_server=None, smtp_port=None, 
                            max_retries=3, retry_delay=5):
    """
    Sends an email notification with optional configuration overrides.
    """
    # Resolve configuration (Argument -> Environment Variable -> Global Variable -> Default)
    sender_email = sender or os.environ.get("EMAIL_SENDER") or globals().get("EMAIL_SENDER")
    receiver_email = receiver or os.environ.get("EMAIL_RECEIVER") or globals().get("EMAIL_RECEIVER")
    cc_email = cc or os.environ.get("EMAIL_CC") or globals().get("EMAIL_CC")
    email_password = password or os.environ.get("EMAIL_PASSWORD") or globals().get("EMAIL_PASSWORD")
    
    # Clean password (remove spaces if copied directly from Google)
    if email_password:
        email_password = email_password.replace(" ", "")
        
    server_address = smtp_server or os.environ.get("SMTP_SERVER") or globals().get("SMTP_SERVER", "smtp.gmail.com")
    server_port = smtp_port or int(os.environ.get("SMTP_PORT") or globals().get("SMTP_PORT", 587))

    if not all([sender_email, receiver_email, email_password]):
        logging.warning("Email configuration missing (Sender, Receiver, or Password). Skipping notification.")
        return

    # Prepare the message
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        if cc_email:
            msg['Cc'] = cc_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{os.path.basename(attachment_path)}"',
            )
            msg.attach(part)
            
        # Attempt to send with retries
        for attempt in range(max_retries):
            try:
                with smtplib.SMTP(server_address, server_port) as server:
                    server.starttls()
                    server.login(sender_email, email_password)
                    server.send_message(msg)
                logging.info(f"Email notification sent: {subject}")
                return
            except Exception as e:
                logging.warning(f"Email send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        logging.error(f"Failed to send email notification after {max_retries} attempts.")

    except Exception as e:
        logging.error(f"Error preparing email: {e}")

def process_single_pdf(driver, pdf_path, sections_to_visit):
    """
    Uploads and processes a single PDF file within an existing browser session.
 
    Args:
        driver: The active Selenium webdriver instance.
        pdf_path (str): The absolute path to the PDF file to upload.
        sections_to_visit (dict): A dictionary of section names and their URL keys.
    """
    log_data = {}
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = start_time # Initialize end_time
    retries = 0
    try:
        logging.debug(f"On upload page: {driver.title}")
 
        # --- PDF Upload ---
        max_upload_retries = 3
        for attempt in range(max_upload_retries):
            retries = attempt
            try:
                # Find the file input element. The ID 'pdf_file' is assumed from context.
                file_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@type='file']")))
                file_input.send_keys(pdf_path) # This sends the file path to the input element
                logging.debug(f"Uploading file: {pdf_path}")
                
                # Give a moment for the file to be attached
                time.sleep(2)

                start_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Start Review']")))
                driver.execute_script("arguments[0].click();", start_btn)

                # Wait for the first section page to load (assuming it redirects to 'subject')
                WebDriverWait(driver, 120).until(EC.title_contains("Section to Review"))
                logging.debug(f"Initial page after upload: {driver.title}")
                break
            except Exception as e:
                logging.warning(f"Upload failed on attempt {attempt + 1}: {e}")
                if attempt < max_upload_retries - 1:
                    logging.info("Retrying upload in 5 seconds...")
                    time.sleep(5)
                    driver.refresh()
                else:
                    raise
 
        # --- Iterate through sections ---
        for display_name, section_key in sections_to_visit.items():
            logging.debug(f"Navigating to section: {display_name}")
            log_data[display_name] = []
           
            try:
                # 1. Find the link and wait for it to be clickable (robust check)
                section_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f"//a[normalize-space()='{display_name}']"))
                )
 
                # --- 🔑 IMPLEMENTATION FIX: Scrolling and Robust Click ---
                # Scroll the element into the center of the viewport before clicking
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", section_link)
                time.sleep(0.5) # Give a short moment for the scroll to complete/page to re-render
 
                try:
                    section_link.click() # Attempt the normal click first
                except Exception as click_e:
                    # Fallback to JavaScript click if regular click still fails
                    logging.warning(f"  ⚠️ Warning: Normal click failed ({click_e.__class__.__name__}), trying JavaScript click.")
                    driver.execute_script("arguments[0].click();", section_link)
                # --- 🔑 END IMPLEMENTATION FIX ---
                   
                # 2. Wait for the page for that section to load
                if section_key == 'custom_analysis':
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".prompt-suggestion-btn"))
                    )
                else:
                    WebDriverWait(driver, 20).until(EC.title_contains(display_name))
                logging.debug(f"On page: {driver.title}")
 
                # --- 🔑 IMPLEMENTATION FIX: Wait for validation messages to appear ---
                # logging.debug("  Waiting 30 seconds for validations to populate...")
                # time.sleep(30)
                validation_messages = driver.find_elements(By.CSS_SELECTOR, "#validation-container .validation-message")
               
                if validation_messages:
                    for msg_element in validation_messages:
                        message_text = msg_element.text.strip()
                        logging.info(f"[{os.path.basename(pdf_path)}] {display_name}: {message_text}")
                        log_data[display_name].append((datetime.now().strftime("%H:%M:%S"), message_text))
                        
                        # --- DATASET WRITER INTEGRATION ---
                        # Save every validation message for the classification model training
                        save_validation_sample(
                            pdf=pdf_path,
                            section=display_name,
                            message=message_text
                        )
                else:
                    logging.debug("  No validation messages found.")
                # --- 🔑 END IMPLEMENTATION FIX ---
 
            except Exception as section_e:
                error_message = f"Could not process section '{display_name}': {section_e}"
                logging.error(f"  [ERROR] {error_message}")
                log_data[display_name].append((datetime.now().strftime("%H:%M:%S"), f"ERROR: {error_message}"))
 
            # Special handling for Custom Analysis page
            if section_key == 'custom_analysis':
                logging.debug("  Running custom analysis prompts...")
 
                # --- 🔑 IMPLEMENTATION FIX: Robustly handling Custom Analysis clicks ---
                # We need to re-find the buttons in each iteration because the page changes after each analysis.
                # A wait is necessary before running the prompts to ensure the page is fully ready.
 
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".prompt-suggestion-btn"))
                    )
                    prompt_texts = [btn.text for btn in driver.find_elements(By.CSS_SELECTOR, ".prompt-suggestion-btn")]
 
                    for i in range(len(prompt_texts)):
                        # Re-find the buttons in each iteration to avoid stale element references.
                        prompt_buttons = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".prompt-suggestion-btn"))
                        )
                        prompt_text = prompt_buttons[i].text
                        logging.debug(f"    Testing prompt: {prompt_text}")
 
                        # Scroll the button into view before clicking
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", prompt_buttons[i])
                        time.sleep(0.5)
 
                        prompt_buttons[i].click()
 
                        # Click the "Run Custom Analysis" button (wait for it to be clickable)
                        run_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn-submit"))
                        )
                        driver.execute_script("arguments[0].click();", run_button) # Use JS click for reliability
 
                        # --- 🔑 IMPLEMENTATION FIX: Wait for validation messages to appear ---
                        # logging.debug("  Waiting 30 seconds for validations to populate...")
                        # time.sleep(30)
                        validation_messages = driver.find_elements(By.CSS_SELECTOR, "#validation-container .validation-message")
                       
                        if validation_messages:
                            for msg_element in validation_messages:
                                message_text = msg_element.text.strip()
                                logging.info(f"[{os.path.basename(pdf_path)}] {display_name}: {message_text}")
                                log_data[display_name].append((datetime.now().strftime("%H:%M:%S"), message_text))

                                # --- DATASET WRITER INTEGRATION for LLM ---
                                # Parse prompt and response to save for LLM fine-tuning
                                if message_text.startswith("Prompt '"):
                                    try:
                                        parts = message_text.split(":", 1)
                                        prompt_part = parts[0]
                                        output_part = parts[1].strip()
                                        prompt_name = prompt_part.replace("Prompt '", "").replace("'", "")
                                        save_analysis_sample(
                                            pdf=pdf_path,
                                            prompt=f"Analyze the appraisal report for the following: {prompt_name}",
                                            output=output_part
                                        )
                                    except IndexError:
                                        logging.warning(f"  ⚠️ Warning: Could not parse custom analysis prompt for dataset: {message_text}")
                        else:
                            logging.debug("  No validation messages found.")
                        # --- 🔑 END IMPLEMENTATION FIX ---
 
                except Exception as custom_analysis_e:
                    error_msg = f"An error occurred during Custom Analysis: {custom_analysis_e}"
                    logging.error(f"  [ERROR] {error_msg}")
                    log_data[display_name].append((datetime.now().strftime("%H:%M:%S"), f"ERROR: {error_msg}"))
                # --- 🔑 END IMPLEMENTATION FIX for Custom Analysis ---
       
        # --- Finish Review ---
        logging.debug("Finishing review for the current document...")
        finish_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Finish Review']"))
        )
        driver.execute_script("arguments[0].click();", finish_button)
        # Wait to be redirected back to the upload page for the next PDF
        WebDriverWait(driver, 20).until(EC.title_contains("Full File Review"))
        logging.debug("Review finished. Ready for next file.")
        return True
 
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False
    finally:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Create the log report at the very end
        report_dir = os.environ.get("REPORT_DIR", ".")
        report_file_path = os.path.join(report_dir, "review_log.txt")
        if log_data:
            create_log_report(log_data, start_time, end_time, pdf_path, report_file_path, retries=retries)
 
def perform_logout(driver):
    """
    Logs out of the application.
    """
    try:
        logging.debug("Logging out...")
        # Assuming a standard 'Logout' link exists in the navigation
        logout_link = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='Logout']"))
        )
        driver.execute_script("arguments[0].click();", logout_link)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "id_username")))
        logging.info("Logout successful.")
    except Exception as e:
        logging.warning(f"Logout failed: {e}")

def process_pdf_task(pdf_filename, pdf_directory, website_url, sections):
    """
    Worker function to process a single PDF in a separate thread/driver.
    """
    absolute_pdf_path = os.path.join(pdf_directory, pdf_filename)
    logging.debug(f"Starting processing for: {pdf_filename}")
    
    MAX_RETRIES = 1
    success = False
    
    for attempt in range(MAX_RETRIES + 1):
        driver = None
        timer = None
        # Set a timeout for the entire task (e.g., 20 minutes) to prevent hanging
        TASK_TIMEOUT = 1200
        timed_out = False

        try:
            # --- Browser Setup ---
            options = webdriver.ChromeOptions()
            # options.add_argument('--headless') # Run in background
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("window-size=1920,1080")
            # Suppress Selenium/Chrome logs
            options.add_argument("--log-level=3")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
            service = ChromeService(ChromeDriverManager().install(), log_output=os.devnull)
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(120)
            
            # --- Timeout Timer ---
            # This will kill the driver if the task takes too long, causing an exception in the main thread
            def timeout_handler():
                nonlocal timed_out
                timed_out = True
                logging.warning(f"Task timed out for {pdf_filename} after {TASK_TIMEOUT} seconds. Terminating driver.")
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            timer = threading.Timer(TASK_TIMEOUT, timeout_handler)
            timer.start()

            # --- Login ---
            driver.get(website_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@id='id_username']")))
            driver.find_element(By.XPATH, "//input[@id='id_username']").send_keys(username)
            driver.find_element(By.XPATH, "//input[@id='id_password']").send_keys(password)
            driver.find_element(By.XPATH, "//button[normalize-space()='Login']").click()
            WebDriverWait(driver, 10).until(EC.title_contains("Review"))
            
            # --- Process PDF ---
            success = process_single_pdf(driver, absolute_pdf_path, sections)
            
            if timed_out:
                raise TimeoutError("Task timed out during processing")
            
            if success:
                try:
                    processed_dir = os.path.join(pdf_directory, "processed")
                    os.makedirs(processed_dir, exist_ok=True)
                    shutil.move(absolute_pdf_path, os.path.join(processed_dir, pdf_filename))
                    logging.info(f"Moved {pdf_filename} to {processed_dir}")
                except Exception as e:
                    logging.error(f"Failed to move {pdf_filename} to processed directory: {e}")

                perform_logout(driver)

            break # Success
            
        except Exception as e:
            logging.error(f"Error processing {pdf_filename}: {e}")
            if timed_out:
                if attempt < MAX_RETRIES:
                    logging.info(f"Timeout occurred for {pdf_filename}. Retrying (Attempt {attempt + 2}/{MAX_RETRIES + 1})...")
                    continue
                else:
                    logging.error(f"Max retries reached for {pdf_filename} due to timeout.")
            break
        finally:
            if timer:
                timer.cancel()
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    if not success:
        try:
            failed_dir = os.path.join(pdf_directory, "failed")
            os.makedirs(failed_dir, exist_ok=True)
            if os.path.exists(absolute_pdf_path):
                shutil.move(absolute_pdf_path, os.path.join(failed_dir, pdf_filename))
                logging.info(f"Moved {pdf_filename} to {failed_dir}")
        except Exception as e:
            logging.error(f"Failed to move {pdf_filename} to failed directory: {e}")

    return success

if __name__ == "__main__":
    setup_logging() # Call this first to set up logging

    website_url = "http://127.0.0.1:8000/login/"
    # Define all sections you might want to visit.
    sections = {
        "Subject": "subject",
        "Base Info": "base_info",
        "Contract": "contract",
        "Neighborhood": "neighborhood",
        # "Site": "site",
        # "Improvements": "improvements",
        # "Sales Grid Adjustment": "sales_grid_adjustment",
        # "Sales Grid": "sales_grid",
        # "Rental Grid": "rental_grid",
        # "Sale History": "sale_history",
        # "Reconciliation": "reconciliation",
        # "Cost Approach": "cost_approach",
        # "Income Approach": "income_approach",
        # "Report Details": "report_details",
        # "Pud Info": "pud_info",
        # "Certification": "certification",
        # "Market Conditions": "market_conditions",
        # "Condo": "condo",
        # "State Requirement": "state_requirement",
        # "Client Lender Requirements": "client_lender_requirements",
        # "Escalation Check": "escalation_check",
        "Custom Analysis": "custom_analysis"
    }
 
    # Get PDF directory from environment variable or use a default 'pdfs' folder.
    pdf_directory = os.environ.get("PDF_DIR", "pdfs")
    absolute_pdf_dir = os.path.abspath(pdf_directory)
 
    if not os.path.isdir(absolute_pdf_dir):
        logging.error(f"PDF directory not found at {absolute_pdf_dir}")
        exit(1)
 
    # Find all PDF files in the specified directory
    all_pdf_files = [f for f in os.listdir(absolute_pdf_dir) if f.lower().endswith('.pdf')]

    # Determine report path to check for existing entries
    report_dir = os.environ.get("REPORT_DIR", ".")
    report_file_path = os.path.join(report_dir, "review_log.txt")
    
    processed_files = get_processed_files(report_file_path)
    
    pdf_files_to_process = [f for f in all_pdf_files if f not in processed_files]
    
    if len(all_pdf_files) > len(pdf_files_to_process):
        logging.info(f"Skipping {len(all_pdf_files) - len(pdf_files_to_process)} files already present in {report_file_path}")
 
    if not pdf_files_to_process:
        logging.info(f"No new PDF files to process in directory: {absolute_pdf_dir}")
        exit(0)
 
    logging.info(f"Found {len(pdf_files_to_process)} PDF(s) to process in '{absolute_pdf_dir}':")
    for pdf in pdf_files_to_process:
        logging.info(f"- {pdf}")
 
    # --- Parallel Processing ---
    # Adjust max_workers based on your system's capabilities (CPU/RAM)
    MAX_WORKERS = 4 
    logging.info(f"Starting parallel processing with {MAX_WORKERS} workers...")
    
    start_time = datetime.now()
    
    successful_files = []
    failed_files = []
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_pdf = {
                executor.submit(process_pdf_task, pdf, absolute_pdf_dir, website_url, sections): pdf
                for pdf in pdf_files_to_process
            }
            
            for future in concurrent.futures.as_completed(future_to_pdf):
                pdf_name = future_to_pdf[future]
                try:
                    if future.result():
                        successful_files.append(pdf_name)
                    else:
                        failed_files.append(pdf_name)
                except Exception as e:
                    logging.error(f"Task for {pdf_name} failed with exception: {e}")
                    failed_files.append(pdf_name)

        duration = datetime.now() - start_time
        
        # Construct Email Body
        email_body = f"Batch Processing Report\n"
        email_body += f"=======================\n\n"
        email_body += f"Total Files: {len(pdf_files_to_process)}\n"
        email_body += f"Duration: {duration}\n"
        email_body += f"Successful: {len(successful_files)}\n"
        email_body += f"Failed: {len(failed_files)}\n\n"
        
        if failed_files:
            email_body += "Failed Files:\n"
            for f in failed_files:
                email_body += f"- {f}\n"
        else:
            email_body += "All files processed successfully.\n"
            
        send_email_notification(
            f"Batch Processing Complete - {len(successful_files)}/{len(pdf_files_to_process)} Success",
            email_body,
            attachment_path=report_file_path
        )

    except Exception as e:
        logging.error(f"Critical error in batch execution: {e}")
        send_email_notification(
            "Critical Error in Batch Processing",
            f"The batch run encountered a critical error and stopped.\nError: {e}",
            attachment_path=report_file_path
        )

    logging.info("All reviews complete.")