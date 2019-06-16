from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType

def custom_extract_url(url):
    return udf(url.replace('http://', '').replace('https://', '').replace('www.',''), StringType());

def custom_is_link(url):
    return udf(url.startswith(('http://','https://','www.')), BooleanType());
