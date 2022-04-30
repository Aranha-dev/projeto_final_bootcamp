## Criação do Banco Cassandra (desafio_agricultura):

create keyspace if not exists desafio_agricultura; 
with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


## Criação das tabelas:

## Tabela pib_agricola:

## Essa tabela irá receber os valores do CSV do PIB Brasileiro.

## Criação da tabela de PIB agrícola Brasileiro entre os anos de 1996 - 2020:
    
CREATE TABLE IF NOT EXISTS "desafio_agricultura"."pib_agricola" (
	id_pib int  primary key,
	ano_pib text, 
	insumo float,
	agropecuaria float,
	industria float,
	servicos float,
	total float 
);

## Essa tabela receberá os valores do CSV do USDA (Departamento de Agricultura dos Estados Unidos)

## Tabela da usa_agricultura:

create table if not exists "desafio_agricultura"."usa_agricultura" (
	id_usa int primary key,
	ano_usa text, 
	valores float
);


## Essa tabela irá receber os valores do CSV novo_valor_producao(FAO).

## Tabela da valor_producao:

create table if not exists "desafio_agricultura"."valor_producao"(
	id_producao int primary key,
	pais text, 
	item text,
	ano_producao text,
	valor_item float
);


## Essa tabela irá receber os valores do CSV quantidade_colheita.

## Tabela da quantidade_colheita:

create table if not exists "desafio_agricultura"."quantidade_colheita"(
	id_quantidade int primary key,
	pais text, 
	item text,
	elemento text,
	ano_quantidade text,
	unidade text,
	valor_quantidade float
);


## As tabelas exportacao_pais e total_exportacao irão receber os dados do CSV exportacaoXpais.

## Tabela da exportacao_pais:

create table if not exists "desafio_agricultura"."exportacao_pais"(
	pais_exportacao text, 
	valor_exportacao float,
	ano_exportacao text,
	primary key (pais_exportacao,ano_exportacao)
);

## Tabela do total_exportacao:

create table if not exists "desafio_agricultura"."total_exportacao"(
	id_total int primary key, 
	total_exportacao float,
	ano_total text
);


## A tabela exportacao_estado irá receber os dados do CSV exportacao X uf.

## Tabela da exportacao_estado:

create table if not exists "desafio_agricultura"."exportacao_estado"(
	estado text , 
	valor_estado float,
	ano_estado text,
	primary key (estado,ano_estado)
);


## A tabela exportacao_produto irá receber os dados do CSV exportacao X produto.

## Tabela da exportacao_produto:

create table if not exists "desafio_agricultura"."exportacao_produto"(
	produto text, 
	valor_produto float,
	ano_produto text,
	primary key (produto,ano_produto)
);


