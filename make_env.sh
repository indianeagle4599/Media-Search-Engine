conda create -n vlm python=3.12 -y
conda activate vlm

python -m pip install -q -U google-genai
pip install -r requirements.txt