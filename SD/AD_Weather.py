import socket
import argparse

# Función que obtiene el nombre de la ciudad a partir de su nombre en minúscula.
# Si la ciudad no se encuentra en el diccionario, devuelve "Ciudad no encontrada".
def obtener_nombre_ciudad(ciudad):
    return datos_ciudades.get(ciudad.lower(), "Ciudad no encontrada")

def main():
    try:
        # Abrimos el archivo en modo lectura y lo asignamos a la variable "archivo".
        # Se cerrará automáticamente cuando salgamos del ámbito.
        with open('ciudades.txt', 'r') as archivo: 
            # Inicializamos un diccionario para almacenar los datos de las ciudades
            datos_ciudades = {}
            
            # Leemos cada línea del archivo y cargamos los datos en el diccionario
            for linea in archivo:
                # Divide la línea en dos partes usando ':' como separador
                ciudad, valor_str = linea.strip().split(':')
                
                # Convertimos el valor a un entero
                valor = int(valor_str)
                
                # Almacenamos la información en el diccionario
                datos_ciudades[ciudad] = valor
        
        # Configuración del servidor
        parser = argparse.ArgumentParser(description="Servidor de Ciudad")
        parser.add_argument("puerto", type=int, help="Puerto de escucha")
        args = parser.parse_args()

        host = "127.0.0.1"  # Dirección IP del servidor

        # Crear un socket TCP/IP
        servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Enlazamos el socket al host y puerto especificados
        servidor_socket.bind((host, args.puerto))

        # Escuchamos conexiones entrantes (máximo 5 conexiones en espera)
        servidor_socket.listen(5)

        print(f"Servidor escuchando en {host}:{args.puerto}")

        while True:
            try:
                # Aceptamos una conexión entrante
                cliente_socket, cliente_direccion = servidor_socket.accept()
                print(f"Conexión entrante de {cliente_direccion}")

                # Recibimos datos del cliente y pasarlos al formato del fichero (minúscula)
                datos = cliente_socket.recv(1024).decode("utf-8").strip().lower()

                if datos:
                    print(f"Peticion recibida: {datos}")
                    # Definimos ciudades válidas basadas en las claves del diccionario
                    ciudades_validas = datos_ciudades.keys()
                    # Verificamos si la ciudad solicitada es válida
                    ciudad = obtener_nombre_ciudad(datos)
                    if ciudad != "Ciudad no encontrada" and ciudad in ciudades_validas:
                            respuesta = f"Nombre de la ciudad: {ciudad}" # Aquí habría que hacer todo lo del tiempo si es valido o no.
                    else:
                        respuesta = "Ciudad no válida"
                else:
                    respuesta = "Peticion vacia"

                # Enviar la respuesta al cliente
                cliente_socket.send(respuesta.encode("utf-8"))

                # Cerrar la conexión con el cliente
                cliente_socket.close()
            # Si el usuario detiene manualmente el servidor
            except KeyboardInterrupt:
            print("Servidor detenido por el usuario.")
            break  # Salir del bucle en caso de interrupción manual

    # Si no se encuentra el fichero "ciudades.txt"
    except FileNotFoundError:
        print("Error: El archivo 'ciudades.txt' no se encuentra.")
    
    # Si los grados asignados a una ciudad no pueden convertirse en entero
    except ValueError:
        print("Error: No se pudo convertir un valor a entero.")

    # Si hay problemas en la creación o enlace del socket
    except socket.error as e:
        print(f"Error en el socket: {e}")
    
    # Cuando se produzca algún error sin especificar anteriormente
    except Exception as e:
        print(f"Error desconocido: {e}")
    finally:
    servidor_socket.close()
