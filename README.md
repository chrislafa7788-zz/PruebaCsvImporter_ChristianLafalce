# PruebaCsvImporter_ChristianLafalce

_Bienvenidos a CSVIMPORTER ! By Christian Lafalce_

## Comenzando 🚀

_Para comenzar por favor dirigirse al visual studio, seleccionar la opcion clone repository y añadir la siguiente URL  https://github.com/chrislafa7788/PruebaCsvImporter_ChristianLafalce.git 
Sino tambien se puede descargar la solucion a mano ingresando al link y descargando el proyecto _

## Ejecutando el programa ⚙️

Luego de ejecutar el programa, Como primer paso se ejecuta el script de BD. El programa toma del appsettings.json el URL del archivo a descargar. Lo descarga y luego comienza la inserción 
_El proyecto posee un archvivo appsettings.json en el cual se podran ubicar cuantas URL se deseen, es mas yo mismo cargue algunas para realizar algunas pruebas!_
_Alli tambien encontraras el connection string para conectarse a la BD local._
_En la carpeta Script se encuentra el archivo .sql el cual es ejecutado en la BD._
_Al ejecutar el programa el mismo posee una serie de logs para ir guiando al usuario, bastante didactico, ¿no?_


## Despliegue 📦

_Al momento de descargar el archivo el usuario puede precionar la tecla E o C para cancelar y producir un retry de la descarga. Si el programa detecta que no se ha descargado ningun tramo del archivo este realiza un retry automatico, Este chequeo es realizado cada 4 minutos de inactividad de descarga_


## Construido con 🛠️

.NET Core SDK (reflecting any global.json):

Version:   3.1.402

Runtime Environment:

OS Name:     Windows

OS Version:  10.0.18363

OS Platform: Windows

RID:         win10-x64



.NET Core SDKs installed:

  2.2.108 [C:\Program Files\dotnet\sdk]

  3.1.402 [C:\Program Files\dotnet\sdk]

 

.NET Core runtimes installed:

  Microsoft.AspNetCore.All 2.1.22 [C:\Program Files\dotnet\shared\Microsoft.AspNetCore.All]

  Microsoft.AspNetCore.All 2.2.6 [C:\Program Files\dotnet\shared\Microsoft.AspNetCore.All]

  Microsoft.AspNetCore.App 2.1.22 [C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App]

  Microsoft.AspNetCore.App 2.2.6 [C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App]

  Microsoft.AspNetCore.App 3.1.8 [C:\Program Files\dotnet\shared\Microsoft.AspNetCore.App]

  Microsoft.NETCore.App 2.1.22 [C:\Program Files\dotnet\shared\Microsoft.NETCore.App]

  Microsoft.NETCore.App 2.2.6 [C:\Program Files\dotnet\shared\Microsoft.NETCore.App]

  Microsoft.NETCore.App 3.1.8 [C:\Program Files\dotnet\shared\Microsoft.NETCore.App]

  Microsoft.WindowsDesktop.App 3.1.8 [C:\Program Files\dotnet\shared\Microsoft.WindowsDesktop.App]



## Autor ✒️

* **Christian Javier Lafalce** 




