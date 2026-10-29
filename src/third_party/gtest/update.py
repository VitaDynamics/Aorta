#!/usr/bin/env python3
"""
Download and extract Google Test
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# Google Test 版本信息
GTEST_VERSION = "1.17.0"
GTEST_URL = f"https://github.com/google/googletest/archive/refs/tags/v{GTEST_VERSION}.tar.gz"
GTEST_ARCHIVE = f"googletest-{GTEST_VERSION}.tar.gz"
GTEST_DIR = "googletest"

def download_and_extract():
    """下载并解压 Google Test 源码"""
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 清理旧的源码目录
    if os.path.exists(GTEST_DIR):
        print(f"Removing existing {GTEST_DIR} directory...")
        shutil.rmtree(GTEST_DIR)
    
    # 下载源码包
    print(f"Downloading Google Test v{GTEST_VERSION}...")
    subprocess.run([
        "wget", "-O", GTEST_ARCHIVE, GTEST_URL
    ], check=True)
    
    # 解压源码包
    print(f"Extracting {GTEST_ARCHIVE}...")
    subprocess.run([
        "tar", "-xzf", GTEST_ARCHIVE
    ], check=True)
    
    # 重命名目录
    extracted_dir = f"googletest-{GTEST_VERSION}"
    if os.path.exists(extracted_dir):
        os.rename(extracted_dir, GTEST_DIR)
        print(f"Renamed {extracted_dir} to {GTEST_DIR}")
    
    # 清理下载的压缩包
    if os.path.exists(GTEST_ARCHIVE):
        os.remove(GTEST_ARCHIVE)
        print(f"Cleaned up {GTEST_ARCHIVE}")
    
    print(f"Google Test v{GTEST_VERSION} successfully downloaded and extracted to {GTEST_DIR}/")

if __name__ == "__main__":
    try:
        download_and_extract()
    except subprocess.CalledProcessError as e:
        print(f"Error: Command failed with exit code {e.returncode}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1) 