import subprocess
import sys
import os

def main():
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 切换到项目根目录
    os.chdir(current_dir)
    # 运行streamlit应用
    subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])

if __name__ == "__main__":
    main()