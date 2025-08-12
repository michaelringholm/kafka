from lxml import etree

def validate_xml_against_xsd(xml_string: str, xsd_path: str) -> bool:
    """
    Validates an XML string against an XSD schema file.

    Args:
        xml_string (str): The XML content as a string.
        xsd_path (str): The filesystem path to the XSD schema file.

    Returns:
        bool: True if valid, False if not.
    """
    try:
        # Parse the XML and XSD files
        xml_doc = etree.fromstring(xml_string.encode('utf-8'))
        with open(xsd_path, 'rb') as xsd_file:
            xsd_doc = etree.parse(xsd_file)

        # Create an XMLSchema object
        xml_schema = etree.XMLSchema(xsd_doc)

        # Validate XML against the schema
        is_valid = xml_schema.validate(xml_doc)

        if not is_valid:
            # Print validation errors if any
            for error in xml_schema.error_log:
                print(f"Validation error: {error.message}")

        return is_valid

    except etree.XMLSyntaxError as e:
        print(f"XML Syntax Error: {e}")
        return False
    except etree.XMLSchemaParseError as e:
        print(f"XSD Schema Parse Error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error during validation: {e}")
        return False


# Example usage:
xml_example = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
   <soapenv:Header/>
   <soapenv:Body>
      <m:GetPrice xmlns:m="https://www.example.org/stock">
         <m:StockName>IBM</m:StockName>
      </m:GetPrice>
   </soapenv:Body>
</soapenv:Envelope>"""

xsd_file_path = 'sample_soap_schema.xsd'

is_valid = validate_xml_against_xsd(xml_example, xsd_file_path)
print("Valid XML" if is_valid else "Invalid XML")
