import requests
from logger_factory import create_logger

class SOAPClient:    
    def __init__(self):
        self.log = create_logger(__name__)
        self.setup_endpoint()        

    def setup_endpoint(self):
        self.SOAP_ENDPOINT = "http://localhost:5000/soap"  # Change to your endpoint

        # Read a sample SOAP XML request from a file
        with open("./xml/sample_soap_client_request.xml", "r") as file:
            self.soap_request = file.read()

        self.headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "GetPrice"
        }

    def call_soap(self):
        self.log.info("🚀 Sending SOAP request to %s", self.SOAP_ENDPOINT)
        try:
            response = requests.post(self.SOAP_ENDPOINT, data=self.soap_request, headers=self.headers, timeout=5)
            self.log.info("📡 HTTP Status: %s", response.status_code)
            
            if response.status_code == 200:
                self.log.info("✅ SOAP Response Received:\n%s", response.text)
            if response.status_code == 202:
                self.log.info("✅ SOAP Response Received, but still processing async:\n%s", response.text)
            else:
                self.log.warning("⚠️ Non-200 response:\n%s", response.text)
        
        except requests.exceptions.RequestException as e:
            self.log.error("💥 Request failed: %s", e)

if __name__ == "__main__":
    client = SOAPClient()
    client.call_soap()
    
