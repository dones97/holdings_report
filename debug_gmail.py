from dotenv import load_dotenv
import os
load_dotenv()
addr = os.getenv('GMAIL_ADDRESS', '')
pwd = os.getenv('GMAIL_APP_PASSWORD', '')
print(f'GMAIL_ADDRESS  : [{addr}]')
print(f'Password length: {len(pwd)} chars')
print(f'Has spaces     : {" " in pwd}')
print(f'Repr           : {repr(pwd)}')
