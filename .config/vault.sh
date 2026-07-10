# Check if the required tools 'jq' and 'yq' are installed
if ! command -v jq &> /dev/null || ! command -v yq &> /dev/null; then
    echo "Error: 'jq' or 'yq' is not installed. Please install them first."
    exit 1
fi

# Check if all required arguments are provided
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <vault_token> <vault_addr> <project_name> <env_version> <destination>"
    exit 1
fi

# Assign arguments to variables
vault_token="$1"
vault_addr="$2"
project_name="$3"
env_version="$4"
destination="$5"

# Fetch JSON data from the Vault API
response=$(curl -s -H "X-Vault-Token: $vault_token" -X GET "$vault_addr/v1/helm/data/$project_name?version=$env_version")
# Extract data from the response
helm_json=$(echo "$response" | jq -r '.data.data')

# Convert JSON to YAML using yq directly
echo "$helm_json" | yq eval -P > "$destination"
