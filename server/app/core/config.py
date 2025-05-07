from dotenv import load_dotenv
from pathlib import Path

current_dir = Path(__file__).parent

env_path = current_dir.parent.parent / ".env"

print(env_path)
print(env_path.exists())
load_dotenv(env_path)
