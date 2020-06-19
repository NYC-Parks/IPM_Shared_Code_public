from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from gspread import Client
from gspread_dataframe import get_as_dataframe
from gspread_dataframe import set_with_dataframe
import re

def google_sheet_auth(cred_file):

    #Provide the scopes, these should be the only scopes required for reading/writing data frames.
    #Consult this page if issues arise: https://developers.google.com/identity/protocols/oauth2/scopes
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive',
             'https://www.googleapis.com/auth/spreadsheets']

    #Get the service account credentials and apply the appropriate scopes
    creds = service_account.Credentials.from_service_account_file(cred_file).with_scopes(scope)

    #Create the session for the google API call, without this the call will be rejected and an error will be thrown
    authed_session = AuthorizedSession(creds)

    #Set up the communication between the client and the google api
    client = Client(creds, authed_session)

    return client

def open_google_worksheet(cred_file, sheet_name, worksheet_name):
    #Check the input paremeter types
    if not isinstance(sheet_name, str):
        raise TypeError('sheet_name must be a string')

    if not isinstance(worksheet_name, str):
        raise TypeError('worksheet_name must be a string')

    #Obtain and authorize the google api credentials
    client = google_sheet_auth(cred_file)

    #Open the google sheet
    sheet = client.open(sheet_name)

    #Open the worksheet (aka tab) of the google sheet
    ws = sheet.worksheet(worksheet_name)

    return ws

def read_google_sheet(cred_file, sheet_name, worksheet_name, evaluate_formulas = True, header = None, drop_empty_cols = True, **options):

    #if not isinstance(evaluate_formulas, bool):
    #    raise TypeError('evaluate_formulas must be a boolean or True/False value')

    if not isinstance(drop_empty_cols, bool):
        raise TypeError('drop_empty_cols must be a boolean or True/False value')

    #Obtain and authorize the google api credentials, then connect to the specific worksheet
    ws = open_google_worksheet(cred_file, sheet_name, worksheet_name)

    #Pull the data from the specified worksheet
    #See additional documentation here: https://pypi.org/project/gspread-dataframe/
    #https://pythonhosted.org/gspread-dataframe/
    google_df = get_as_dataframe(ws, evaluate_formulas = evaluate_formulas, header= header, **options)

    if drop_empty_cols = True:
        #Define the regular expression to identify the columns with no data (named Unnamed: n)
        r = re.compile('^Unnamed:.')

        #Find the columns matching the above expression and add them to a list
        drop_cols = [col for col in list(google_df.columns.values) if re.match(r, col) != None]

        #Drop the columns (in place) with no data if they exist
        if len(drop_cols) > 0:
            google_df.drop(columns = drop_cols, inplace = True)

    return google_df

def write_google_sheet(dataframe, cred_file, sheet_name, worksheet_name, row = 1, col = 1, include_index = False,
                       include_column_header = True, resize = True, allow_formulas = True):

    #if not isinstance(include_index, bool):
    #    raise TypeError('include_index must be a boolean or True/False value')

    #if not isinstance(include_column_header, bool):
    #    raise TypeError('include_column_header must be a boolean or True/False value')

    #if not isinstance(allow_formulas, bool):
    #    raise TypeError('allow_formulas must be a boolean or True/False value')

    #if not isinstance(resize, bool):
    #    raise TypeError('resize must be a boolean or True/False value')

    #Obtain and authorize the google api credentials, then connect to the specific worksheet
    ws = open_google_worksheet(cred_file, sheet_name, worksheet_name)

    #See additional documentation here: https://pypi.org/project/gspread-dataframe/
    #https://pythonhosted.org/gspread-dataframe/
    set_with_dataframe(worksheet = ws, dataframe = dataframe, row = row, col = col,
                       include_index = include_index, include_column_header = include_column_header,
                       resize = resize, allow_formulas = allow_formulas)
