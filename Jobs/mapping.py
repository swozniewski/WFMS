#number formats: Boolean, Bytes, Color, Duration, Number, Percentage, Static lookup, String, Url

mapping = {
    "mappings": {
        "properties": {
            "modificationtime": {"type": "date", "format": "date_optional_time"}
        }
    }
}

bytetypes = ['inputfilebytes', 'outputfilebytes', 'avgvmem', 'avgswap', 'IObytesRead',
    'IObytesWritten', 'IObytesReadRate', 'IObytesWriteRate', 'diskio', 'maxswap']
for entry in bytetypes:
    mapping['mappings']['properties'][entry] = {'type': 'number', 'format': 'Bytes'}
