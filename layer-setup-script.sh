#!/bin/bash
# Script to create AWS Lambda Layer for Google Cloud Storage

# Create directory structure
echo "Creating directory structure..."
mkdir -p google-cloud-layer/python
cd google-cloud-layer

# Create requirements.txt
echo "Creating requirements.txt..."
cat > requirements.txt << EOL
google-cloud-storage==2.12.0
google-auth==2.23.0
tenacity==8.2.3
EOL

# Install packages
echo "Installing Python packages to the layer directory..."
pip install -r requirements.txt -t python/ --no-cache-dir

# Remove unnecessary files to reduce size
echo "Cleaning up unnecessary files..."
find python/ -name "*.pyc" -delete
find python/ -name "__pycache__" -delete
find python/ -name "*.dist-info" -exec rm -rf {} +
find python/ -name "*.egg-info" -exec rm -rf {} +

# Create ZIP archive
echo "Creating ZIP archive..."
zip -r google-cloud-layer.zip python/

echo "Layer ZIP file created: $(pwd)/google-cloud-layer.zip"
echo "Upload this file to AWS Lambda as a layer"
