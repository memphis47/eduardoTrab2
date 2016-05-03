#Introdução 
 
Relatório   para   a   disciplina   ​
CI763   (Gerenciamento   de   Dados   Distribuídos)   ​
sobre   o  
trabalho Tabela Hash Distribuída (DHT). 
O   trabalho   tem   como   objetivo   implementar   uma   tabela   hash   distribuida   seguindo   as  
regras descritas na pagina da disciplina. 
 
#Implementação 
 
O   trabalho   foi   implementado   na   linguagem   de   programação   Ruby   e   consiste   em  
quatro partes: 
 
1. Lê   o   STDIN   e   armazena   o   mesmo   em   um   ​
Array   ​
de   processamento.   Utiliza­se  
dessa forma para evitar acessos desnecessários à I/O. 
2. Após   a   criação   desse   ​
Array   ​
de   processamento   o   programa   começa   a  
verificar   os   comandos   recebido   e   começa   a   implentar   a   hash   table,   seguindo  
as regras de cada comando como E,I,S ou L.  
A   cada   processamento   da   operação   L   escreve   o   resultado   tal   qual   descrito  
no STDOUT. 
 
#Compilação e Execução 
Para compilar basta executar make: 
 
        $ make 
 
Para a execução do Programa basta executar:  
 
        $ ./mydht <teste.in> <teste.out> 
 
 
#Bugs 
O programa não possui nenhum Bug Conhecido. 
