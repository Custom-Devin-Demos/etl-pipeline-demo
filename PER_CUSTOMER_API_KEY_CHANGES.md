# Per-Customer API Key Selection Changes

## Summary
This document describes the changes made to `.github/workflows/devin-scan.yml` to support per-customer Devin API key selection and fix the SonarCloud project key.

## Changes Made

### 1. Added Customer Input to Workflow Dispatch
Added a new `customer` input parameter to the workflow dispatch trigger:
```yaml
customer:
  description: 'Customer slug for per-customer Devin API key selection'
  required: false
  default: 'default'
```

### 2. Updated Environment Variables
Replaced the single `DEVIN_API_KEY` with per-customer keys:
```yaml
DEVIN_API_KEY_DEFAULT: ${{ secrets.DEVIN_API_KEY }}
DEVIN_API_KEY_WAYFAIR: ${{ secrets.DEVIN_API_KEY_WAYFAIR }}
DEVIN_API_KEY_ZAXBYS: ${{ secrets.DEVIN_API_KEY_ZAXBYS }}
CUSTOMER: ${{ github.event.inputs.customer }}
```

### 3. Added API Key Selection Logic
Added a case statement to select the appropriate API key based on customer input:
```bash
# Select per-customer Devin API key
case "$CUSTOMER" in
  wayfair) DEVIN_API_KEY="$DEVIN_API_KEY_WAYFAIR" ;;
  zaxbys)  DEVIN_API_KEY="$DEVIN_API_KEY_ZAXBYS" ;;
  *)       DEVIN_API_KEY="$DEVIN_API_KEY_DEFAULT" ;;
esac

if [ -z "$DEVIN_API_KEY" ]; then
  echo "ERROR: No Devin API key found for customer: $CUSTOMER"
  exit 1
fi

echo "Using Devin API key for customer: $CUSTOMER"
```

### 4. Fixed SonarCloud Project Key
Updated all SonarCloud project key references from `COG-GTM_etl-pipeline-demo` to `Custom-Devin-Demos_etl-pipeline-demo` in 5 locations:
- Line 47: qualitygates/project_status
- Line 121: issues/search
- Line 123: qualitygates/project_status
- Line 129: hotspots/search
- Line 262: sonarcloud.io/dashboard

## Required Secrets
The following GitHub Actions secrets must be added:
- `DEVIN_API_KEY` - Default/fallback Devin API key
- `DEVIN_API_KEY_WAYFAIR` - Devin API key for Wayfair org
- `DEVIN_API_KEY_ZAXBYS` - Devin API key for Zaxby's org

## Usage
The external app can now trigger the workflow with a customer parameter:
```bash
# For Wayfair
customer=wayfair

# For Zaxby's  
customer=zaxbys

# For default
customer=default
```

The workflow will automatically select the appropriate Devin API key based on the customer input.