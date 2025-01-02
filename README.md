# ETL
Curso Alura de Pipeline de dados onde utilizando o WSL criei as pastas que foram utilizadas no VS Code e utilizando uma máquina virtual criei os notebooks a seguir. 
A ideia é que duas empresas se uniram e agora precisamos fazer o mesmo com seus arquivos. A empresa_A costumava salvar tudo em json e a empresa_b em csv. 
A primeira etapa foi extrair as informações.
Analisando esses arquivos foi possível ver que apenas a empresa_B amntem informações de Data de Venda mas o pessoal de BI pediu para mantê-las para análises específicas. Então, antes de ajustar os nomes das colunas, foi criada uma coluna de Data de Venda para a empresa_A com a informação "Indisponível".
Com esses ajustes feitos, utilizei o concat para unir os dois arquivos em um e salvá-lo em csv.
Fiz um teste utilizando funções para a organização e um segundo utilizando classes e instâncias.
