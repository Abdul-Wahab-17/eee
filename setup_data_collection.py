"""
Setup script for multi-crop data collection.

This script:
1. Creates necessary directories
2. Checks for API key configuration
3. Validates environment setup
4. Provides instructions for data download

Run this BEFORE running 01_download_all_data.py
"""

import os
from pathlib import Path
import sys

def create_directories():
    """Create all necessary data directories."""
    print("Creating data directories...")
    
    directories = [
        "data/raw",
        "data/processed",
        "results/figures",
        "results/tables",
        "models"
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Created: {dir_path}")
    
    print()

def check_api_key():
    """Check if USDA NASS API key is configured."""
    print("Checking API key configuration...")
    
    # Check for .env file
    env_file = Path(".env")
    if env_file.exists():
        print("  ✓ Found .env file")
        with open(env_file) as f:
            content = f.read()
            if "USDA_NASS_API_KEY" in content:
                # Check if it's not the placeholder
                for line in content.split('\n'):
                    if line.startswith("USDA_NASS_API_KEY"):
                        key = line.split('=')[1].strip()
                        if key and key != "your_api_key_here":
                            print(f"  ✓ API key configured: {key[:10]}...")
                            return True
                        else:
                            print("  ✗ API key is placeholder value")
                            return False
    
    print("  ✗ No .env file found or API key not configured")
    return False

def create_env_template():
    """Create a template .env file."""
    env_file = Path(".env")
    
    if not env_file.exists():
        print("\nCreating .env template file...")
        with open(env_file, 'w') as f:
            f.write("# USDA NASS QuickStats API Key\n")
            f.write("# Get your key from: https://quickstats.nass.usda.gov/api\n")
            f.write("USDA_NASS_API_KEY=your_api_key_here\n")
        print("  ✓ Created .env file")
        print("  → Please edit .env and add your USDA NASS API key")
    else:
        print("\n.env file already exists")

def check_dependencies():
    """Check if required Python packages are installed."""
    print("\nChecking Python dependencies...")
    
    required_packages = [
        'pandas',
        'requests',
        'numpy',
        'tqdm',
        'python-dotenv'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} - NOT INSTALLED")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    
    print("\n✓ All dependencies installed")
    return True

def print_instructions():
    """Print next steps for the user."""
    print("\n" + "="*70)
    print("SETUP COMPLETE - NEXT STEPS")
    print("="*70)
    print()
    print("1. Get your USDA NASS API key:")
    print("   → Visit: https://quickstats.nass.usda.gov/api")
    print("   → Click 'Request API Key'")
    print("   → Check your email for the key")
    print()
    print("2. Add your API key to .env file:")
    print("   → Open .env in a text editor")
    print("   → Replace 'your_api_key_here' with your actual key")
    print("   → Save the file")
    print()
    print("3. Run the data download script:")
    print("   → python scripts/01_download_all_data.py")
    print()
    print("4. Expected download time: 6-10 hours")
    print("   → Recommended: Run overnight")
    print("   → Downloads data for all 12 crops")
    print()
    print("="*70)

def main():
    """Run setup process."""
    print("="*70)
    print("MULTI-CROP DATA COLLECTION SETUP")
    print("="*70)
    print()
    
    # Create directories
    create_directories()
    
    # Check dependencies
    deps_ok = check_dependencies()
    if not deps_ok:
        print("\n⚠️  Please install missing dependencies first")
        print("Run: pip install -r requirements.txt")
        return
    
    # Check/create API key configuration
    api_key_ok = check_api_key()
    
    if not api_key_ok:
        create_env_template()
    
    # Print instructions
    print_instructions()
    
    if api_key_ok:
        print("\n✅ Setup complete! You're ready to download data.")
        print("\nRun: python scripts/01_download_all_data.py")
    else:
        print("\n⚠️  Please configure your API key in .env before downloading data")

if __name__ == "__main__":
    main()
