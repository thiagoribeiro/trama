#!/bin/bash

# 1. Criar um branch temporário sem histórico
git checkout --orphan branch_limpo

# 2. Adicionar todos os ficheiros atuais
git add -A

git rm --cached "$0" 2>/dev/null

# 3. Criar o commit inicial único
git commit -m "Initial commit (cleaned)"

# 4. Eliminar o branch principal antigo
# Nota: Verifique se o seu branch se chama 'main' ou 'master'
git branch -D main

# 5. Renomear o branch atual para main
git branch -m main

# 6. Push forçado para o GitHub
git push -f origin main

echo "Histórico limpo e enviado com sucesso!"