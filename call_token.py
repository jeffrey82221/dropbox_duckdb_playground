"""
Getting Dropbox Refresh Access Token

TODO:
- [ ] enable inserting token during PR.
"""
import requests
import base64
import requests
import os

APP_KEY = os.getenv('DROPBOX_APP_KEY')
APP_SECRET = os.getenv('DROPBOX_APP_SECRET')


def get_refresh_token(access_code_generated):
    BASIC_AUTH = base64.b64encode(f'{APP_KEY}:{APP_SECRET}'.encode())
    headers = {
        'Authorization': f"Basic {BASIC_AUTH}",
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = f'code={access_code_generated}&grant_type=authorization_code'
    response = requests.post('https://api.dropboxapi.com/oauth2/token',
                             data=data,
                             auth=(APP_KEY, APP_SECRET),
                             headers=headers)
    data = response.json()
    if 'refresh_token' in data:
        return data['refresh_token']
    else:
        url = f'https://www.dropbox.com/oauth2/authorize?client_id={APP_KEY}&token_access_type=offline&response_type=code'
        raise ValueError(f'Goto: \n{url}\n and regrenerate access code.')


if __name__ == '__main__':
    import subprocess
    subprocess.call(['sh', './credential.ini'])
    url = f'https://www.dropbox.com/oauth2/authorize?client_id={APP_KEY}&token_access_type=offline&response_type=code'
    print(f'Goto: \n{url}\n and regrenerate access code.')
    dropbox_access_token = input('dropbox_access_token:\n')
    token = get_refresh_token(dropbox_access_token)
    os.environ["DROPBOX_TOKEN"] = token
    print(f'export DROPBOX_TOKEN={token}')
