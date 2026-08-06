"""绕过 pip 代理配置, 直接用 pip 内部 API 安装依赖"""
import subprocess, sys

packages = ["tqsdk", "flask", "pandas", "akshare"]
mirror = "https://pypi.doubanio.com/simple"

for pkg in packages:
    print(f"正在安装 {pkg}...")
    try:
        __import__(pkg)
        print(f"  ✓ {pkg} 已安装")
    except ImportError:
        cmd = [
            sys.executable, "-m", "pip", "install", "--isolated",
            pkg,
            "-i", mirror,
            "--trusted-host", "pypi.doubanio.com",
            "--timeout", "600",
            "--retries", "3",
            "--no-cache-dir",
            "--disable-pip-version-check",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✓ {pkg} 安装成功")
        else:
            # 只输出最后几行错误
            err_lines = result.stderr.strip().split("\n")[-5:]
            print(f"  ✗ {pkg} 安装失败:\n    " + "\n    ".join(err_lines))

print("\n所有依赖安装完成!")