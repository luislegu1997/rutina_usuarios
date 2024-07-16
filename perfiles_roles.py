import sys
import psycopg2


#Antes de ejecutar rutina quitar de .DAT las primeras lineas, dejar apartir de primer perfil a procesar


#loc celaya
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    database="imcelaya_loc",
    user="postgres",
    password="admin"
)


cursor = conn.cursor()


def perfiles_roles(file_path):

    with open(file_path, 'r+') as file:

        for oneline in file:

            lines = oneline.split("N                                                                                                                                            N")

            for line in lines:

                print("----------------")

                line = line.strip()

                line = line.split()

                if len(line) > 1:

                    perfil = line[0]

                    print(f"Perfil: {perfil}")
                
                    roles = line[2].split(',')

                    for rol in roles:

                        qry = "SELECT * FROM keplersc.perfiles_roles where perfil=%s and rol=%s " 
                        cursor.execute(qry, (perfil,rol))
                        res = cursor.fetchall()

                        if len(res) == 0:

                            print(f"rol: {rol}")
                             
                            insert = "insert into keplersc.perfiles_roles(perfil,rol) values(%s,%s)" 
                            cursor.execute(insert, (perfil,rol ))
                            conn.commit()



if __name__ == "__main__":
    file_path = sys.argv[1]
    perfiles_roles(str(file_path))



