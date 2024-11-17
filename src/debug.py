import sys
from pathlib import Path

# Obtém o caminho absoluto do diretório raiz do projeto
project_root = Path(__file__).parents[1]  # Ajuste o número de 'parents' conforme necessário

# Adiciona o diretório 'src' ao sys.path
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

print("Caminhos no sys.path:")
for path in sys.path:
    print(path)

# Opcional: Verificar se o caminho do módulo 'steps' está presente
project_root = Path(__file__).parents[1]
steps_path = project_root / "src"
print(f"\nVerificando se o caminho '{steps_path}' está no sys.path: {str(steps_path) in sys.path}")
