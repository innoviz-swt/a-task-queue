to make .env file take effect in your terminal add following to bashrc
```
[ -f .env ] && grep -q -v -e '^[[:space:]]*$' -e '^#' .env && export $(grep -v -e '^[[:space:]]*$' -e '^#' .env | xargs)
```
