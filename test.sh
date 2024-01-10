[ -f .env ] && grep -v -e '^[[:space:]]*$' -e '^#' .env && export $(grep -v -e '^[[:space:]]*$' -e '^#' .env | xargs)
