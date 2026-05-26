# CMIP Variable Mappings Data Sources

This directory contains CMIP to ACCESS model variable mapping files that enable 
users to search for data using CMIP standard variable names.

## Files

### access-esm1-6-cmip-mappings.json
- **Model**: ACCESS-ESM1.6
- **Purpose**: Maps CMIP variable names to ACCESS model field codes
- **Format**: JSON with atmosphere, land, ocean components
- **Usage**: Used by the aliasing system to translate user searches
- **Example**: `ci` (CMIP) → `fld_s05i269` (ACCESS)

## Structure

Each mapping file should contain:
```json
{
    "model_info": {
        "model_id": "MODEL-NAME",
        "components": ["atmosphere", "land", "ocean"],
        "description": "Description of mappings"
    },
    "atmosphere": {
        "cmip_variable_name": {
            "CF standard Name": "...",
            "model_variables": ["access_field_code"],
            "calculation": {...}
        }
    },
    "land": {...},
    "ocean": {...}
}
```

## Adding New Mappings

To add mappings for new models:
1. Create a new JSON file following the naming convention: `{model-name}-cmip-mappings.json`
2. Follow the structure above
3. Update the loading logic in `aliases.py` if needed
4. Add the file to `pyproject.toml` package data if not using a wildcard pattern