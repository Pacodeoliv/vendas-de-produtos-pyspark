import psycopg2

try:
    # Configurações do banco de dados
    connection = psycopg2.connect(
        host="dpg-ct7jv1ij1k6c73au96tg-a",      # Altere para o endereço do seu servidor, se remoto
        database="dbprojetospark",  # Nome do banco
        user="spark1213",    # Usuário do PostgreSQL
        password="dpg-ct7jv1ij1k6c73au96tg-a.oregon-postgres.render.com"   # Senha do usuário
    )
    print("Conexão bem-sucedida!")
except psycopg2.Error as e:
    print("Erro ao conectar ao PostgreSQL:", e)
finally:
    if 'connection' in locals() and connection:
        connection.close()
