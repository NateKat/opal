import os

# ws server (TODO: merge with opal client config)
OPAL_WS_LOCAL_URL = os.environ.get("OPAL_WS_LOCAL_URL", "ws://localhost:7002/ws")
OPAL_WS_TOKEN = os.environ.get("OPAL_WS_TOKEN", "PJUKkuwiJkKxbIoC4o4cguWxB_2gX6MyATYKc2OCM")
BROADCAST_URI = "postgres://localhost/acalladb"

# git watcher
POLICY_REPO_URL = os.environ.get("POLICY_REPO_URL", None)
POLICY_REPO_CLONE_PATH = os.environ.get("POLICY_REPO_CLONE_PATH", "regoclone")

# github webhook
POLICY_REPO_WEBHOOK_SECRET = os.environ.get("POLICY_REPO_WEBHOOK_SECRET", None)

try:
    POLICY_REPO_POLLING_INTERVAL = int(os.environ.get("POLICY_REPO_POLLING_INTERVAL", "0"))
except ValueError:
    POLICY_REPO_POLLING_INTERVAL = 0

ALLOWED_ORIGINS = ["*"]
OPA_FILE_EXTENSIONS = ('.rego', '.json')