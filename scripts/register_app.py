from mastodon import Mastodon
from pathlib import Path

# 1. Create the app (writes client creds to file)
Mastodon.create_app(
    "MyMastodonDS",
    api_base_url="https://mastodon.social",
    scopes=["read", "write", "follow"],     # adjust as needed
    to_file="config/clientcred.secret"
)

# 2. Log in as your user (writes user token to file)
Mastodon.log_in(
    "youremail@example.com",
    "your_password_here",
    api_base_url="https://mastodon.social",
    to_file="config/usercred.secret"
)

print("✔️  Created config/clientcred.secret and config/usercred.secret")
