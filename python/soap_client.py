import requests
import logging

# Setup logging with emojis
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

SOAP_ENDPOINT = "http://localhost:5000/soap"  # Change to your endpoint

# Sample SOAP request XML
soap_request = """<?xml version="1.0"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:stk="https://www.example.org/stock">
   <soapenv:Header/>
   <soapenv:Body>
      <stk:GetPrice>
         <stk:StockName>ASUS</stk:StockName>
      </stk:GetPrice>
   </soapenv:Body>
</soapenv:Envelope>
"""

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
