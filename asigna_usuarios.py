import psycopg2
import sys



#loc pachuca
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    database="impachuca_loc",
    user="postgres",
    password="admin"
)


cursor = conn.cursor()



def rutina(file_path):

    query = "SELECT * FROM keplersc_mig_core.relacion_usuarios order by clave" 
    cursor.execute(query, ('',))
    usuarios = cursor.fetchall()

    for usr in usuarios:
        clave = usr[0]
        rol = usr[4]
    
        print(f"--USR:----------{clave}-------------")

        query_perfiles = "SELECT perfil1,perfil2,perfil3 FROM keplersc_mig_core.usuarios_perfiles where clave=%s " 
        cursor.execute(query_perfiles, (clave,))
        perfiles = cursor.fetchall()
        
        if len(perfiles) > 0:
            for perfil in perfiles[0]:
                print(f"-------PERFIL:-------{perfil}-------------")

                query_opciones = "SELECT c2 FROM keplersc_mig.kdopcionesperfil where c1=%s " 
                cursor.execute(query_opciones, (perfil,))
                opciones = cursor.fetchall()

                if len(opciones) > 0:
                    for opcion in opciones:

                        with open(file_path, 'r+') as file:

                            lineas = file.readlines()

                            for i, linea in enumerate(lineas):

                                identacion = len(linea) - len(linea.lstrip())

                                linea = linea.split()
                                
                                for x, col in enumerate(linea):

                                    if col[:5] == 'roles':
                                
                                        opciones_col = col.split(',')
                                        if len(opciones_col) > 1:
                                            opcion_col = opciones_col[1].replace('"', "") 
                                            if opcion_col == opcion[0]:


                                                if rol not in opciones_col and rol + '"' not in opciones_col:
                                                    
                                                    print(f'columna antes: {col}')
                                                    print(f'linea antes: {linea}')
                                                    col_nueva = col[:-1] + f',{rol}"'
                                                    print(f'columna nueva: {col_nueva}')
                                                    linea[x] = col_nueva
                                                    print(f'linea nueva: {linea}')
                                                    linea_nueva = " ".join(linea)
                                                    if identacion > 0 :
                                                        linea_nueva = " " * identacion + linea_nueva
                                                    print(linea_nueva)

                                                    #guarda cambios en el archivo
                                                    lineas[i] = linea_nueva + "\n"
                                                    file.seek(0)
                                                    file.writelines(lineas)

                                                    #guarda relacion de roles con opciones
                                                    qry = "SELECT * FROM keplersc_mig_core.kdopcionesroles where rol=%s and opcion=%s " 
                                                    cursor.execute(qry, (rol,opcion[0]))
                                                    res = cursor.fetchall()

                                                    if len(res) == 0:
                                                        
                                                        insert = "insert into keplersc_mig_core.kdopcionesroles(rol,opcion) values(%s,%s)" 
                                                        cursor.execute(insert, (rol,opcion[0] ))
                                                        conn.commit()





                                        



                                        



     
                   
                   








if __name__ == "__main__":
    file_path = sys.argv[1]
    rutina(str(file_path))



