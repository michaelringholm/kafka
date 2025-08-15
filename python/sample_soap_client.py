import requests
import logging

# Setup logging with emojis
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

SOAP_ENDPOINT = "http://localhost:5000/soap"  # Change to your endpoint

# Read a sample SOAP XML request from a file
with open("./xml/sample_soap_client_request.xml", "r") as file:
    soap_request = file.read()

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "GetPrice"
}

def call_soap():
    logging.info("🚀 Sending SOAP request to %s", SOAP_ENDPOINT)
    try:
        response = requests.post(SOAP_ENDPOINT, data=soap_request, headers=headers, timeout=5)
        logging.info("📡 HTTP Status: %s", response.status_code)
        
        if response.status_code == 200:
            logging.info("✅ SOAP Response Received:\n%s", response.text)
        if response.status_code == 202:
            logging.info("✅ SOAP Response Received, but still processing async:\n%s", response.text)
        else:
            logging.warning("⚠️ Non-200 response:\n%s", response.text)
    
    except requests.exceptions.RequestException as e:
        logging.error("💥 Request failed: %s", e)

if __name__ == "__main__":
    call_soap()
