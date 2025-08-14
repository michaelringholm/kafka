<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/">
        <common_trade>
            <currency_code><xsl:value-of select="//ccy"/></currency_code>
            <!-- Add more mappings as needed -->
        </common_trade>
    </xsl:template>
</xsl:stylesheet>