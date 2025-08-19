from typing import Any
import json
import xmltodict
from lxml import etree
import transformations_config_reader

class ContentFormatter:
    """Class to handle data format conversions between XML and JSON."""

    TRANSFORMATIONS = transformations_config_reader.fetch_transformations()  # Fetch transformations from config

    @staticmethod
    def format_data(payload: str, source_format: str, dest_format: str) -> str:
        """
        Formats the payload from source_format to dest_format.
        Args:
            payload: The data to be formatted
            source_format: Format of input data ('xml' or 'json')
            dest_format: Desired output format ('xml' or 'json')
        Returns:
            Formatted data string
        Raises:
            ValueError: If formats are unsupported or payload is invalid
        """
        if not payload:
            raise ValueError("Payload cannot be empty")
            
        supported_formats = ['xml', 'json']
        if source_format not in supported_formats or dest_format not in supported_formats:
            raise ValueError(f"Formats must be one of {supported_formats}")

        try:
            if source_format == "xml" and dest_format == "xml":
                return ContentFormatter._map_xml_to_xml(payload)
            elif source_format == "json" and dest_format == "xml":
                return ContentFormatter._format_json_to_xml(payload)
            elif source_format == "xml" and dest_format == "json":
                return ContentFormatter._format_xml_to_json(payload)
            else:
                return ContentFormatter._format_json_to_json(payload)
        except Exception as e:
            raise ValueError(f"Error converting {source_format} to {dest_format}: {e}")

    @staticmethod
    def _map_xml_to_xml(payload: str) -> str:
        """Transform XML based on source format."""
        try:
            # Parse input XML
            source_doc = etree.fromstring(payload)
            
            # Determine source format from root element
            source_system = source_doc.tag
            
            if source_system in ContentFormatter.TRANSFORMATIONS:
                # Load appropriate XSLT
                xslt_file = ContentFormatter.TRANSFORMATIONS[source_system]
                xslt_doc = etree.parse(xslt_file)
                transform = etree.XSLT(xslt_doc)
                
                # Apply transformation
                result_tree = transform(source_doc)
                return etree.tostring(result_tree, pretty_print=True).decode()
            else:
                raise ValueError(f"No transformation defined for source system: {source_system}")
                
        except Exception as e:
            raise ValueError(f"XML transformation failed: {str(e)}")

    @staticmethod
    def _format_json_to_xml(payload: str) -> str:
        """Convert JSON to XML format."""
        try:
            json_dict = json.loads(payload)
            return xmltodict.unparse({'root': json_dict})
        except Exception as e:
            raise ValueError(f"Invalid JSON payload: {str(e)}")

    @staticmethod 
    def _format_xml_to_json(payload: str) -> str:
        """Convert XML to JSON format."""
        try:
            xml_dict = xmltodict.parse(payload)
            return json.dumps(xml_dict)
        except Exception as e:
            raise ValueError(f"Invalid XML payload: {str(e)}")

    @staticmethod
    def _format_json_to_json(payload: str) -> str:
        """Validate and return JSON payload."""
        try:
            return json.dumps(json.loads(payload))
        except Exception as e:
            raise ValueError(f"Invalid JSON payload: {str(e)}")