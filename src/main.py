import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

from lib.classes.connection_api_google import *
from lib.classes.generate_date import *
from lib.classes.colettor_xml import *
from lib.classes.commun_functions import *
from lib.pipelines.gerar_novos_relacionados import *

pipeline_gerar_novos_arquivos_relacionados()

