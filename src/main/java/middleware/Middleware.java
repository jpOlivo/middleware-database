/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package middleware;
/**
 *
 * @author Juan Alfonso Lara Torralbo
 */

// Importamos el paquete de SQL para Java
import java.sql.*;

public class Middleware {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
         
           // Variables para realizar las conexiónes
           Connection conn1 = null;
           Connection conn2 = null;

           try
           { 
               /******* CONEXIÓN CON LA BBDD *******/
               String userName = "root";
               String password = "";
               
               // En la sentencia siguiente, 3306 es el puerto a usar
               // Se debe utilizar el que se indicó en la instalación de MySQL
               String url = "jdbc:mysql://localhost:3306/base1";
 
               Class.forName ("com.mysql.jdbc.Driver").newInstance ();
               conn1 = DriverManager.getConnection (url, userName, password);
               System.out.println ("Database connection established");  
               Statement instruccion1 = conn1.createStatement();
                
               /******* OBTENCIÓN DEL TAMAÑO DE LA CONSULTA (número de filas) *******/
               ResultSet tabla1 = instruccion1.executeQuery("SELECT count(*) FROM producto");
               tabla1.next();
               int tamaño = tabla1.getInt(1);

               /******* OBTENER LOS DATOS DE LA CONSULTA*******/
               ResultSet tabla = instruccion1.executeQuery("SELECT * FROM producto");

               /******* PASAR LOS DATOS A UNA MATRIZ *******/
               String matriz [] [] = new String [tamaño][4];                     
               // El valor 4 es porque leeremos 4 campos: id, nombre, descripcion y precio

               int i = 0;
               while(tabla.next())
               {
                    matriz [i][0] = tabla.getString(1);
                    matriz [i][1] = tabla.getString(2);
                    matriz [i][2] = tabla.getString(3);
                    i = i + 1;
               }

               /******* INTEGRACIÓN CON LA SEGUNDA FUENTE DE DATOS *******/
               // En la sentencia siguiente, 3306 es el puerto a usar
               // Se debe utilizar el que se indicó en la instalación de MySQL
               url = "jdbc:mysql://localhost:3306/base2";
 
               Class.forName ("com.mysql.jdbc.Driver").newInstance ();
               conn2 = DriverManager.getConnection (url, userName, password);
               System.out.println ("Database connection established");  
               Statement instruccion2 = conn2.createStatement();

               Float precio;
               ResultSet tabla2;
               String consulta2;
               for (i= 0; i <tamaño; i++)
               {    
                    /******* OBTENCIÓN DE LOS DATOS DE LA CONSULTA *******/
                    consulta2 = "SELECT precio FROM producto WHERE id = " + matriz[i][0];
                    tabla2 = instruccion2.executeQuery(consulta2);
                    tabla2.next();
                    
                    precio = tabla2.getFloat(1);
                    matriz [i][3] = precio.toString();
               }    

               /******* SE MUESTRA EL RESULTADO *******/
               System.out.println("Nombre" + "\t" + "\t" + "Precio");
               for (i= 0; i <tamaño; i++)
               {
                    System.out.print(matriz[i][1] + "\t");
                    
                    if (matriz[i][1].length() < 8)
                       System.out.print("\t");     
                        
                    System.out.print(matriz[i][3] + "\n");
               }
                
           // CAPTURA DE EXCEPCIONES
           }
           catch (Exception e) /******* EXCEPCIÓN EN CASO DE ERROR DE CONEXIÓN *******/
           {
        	   e.printStackTrace();
               System.err.println ("Cannot connect to database server");
           }
    }
}