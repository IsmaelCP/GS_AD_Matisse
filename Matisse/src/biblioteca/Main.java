package biblioteca;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.matisse.MtDatabase;
import com.matisse.MtException;
import com.matisse.MtObjectIterator;
import com.matisse.MtPackageObjectFactory;

public class Main {
	
	// Método para Crear objetos en la base de datos
		public static void creaObjetos(String hostname, String dbname) {
			try {
				// Abre la BD con el Hostname (localhost), dbname (biblioteca) y el namespace "biblioteca"
				MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
				db.open();
				db.startTransaction();
				System.out.println("Conectando a la base de datos " + db.toString() + " de Matisse.");

				// Crea un objeto Autor
				Autor a1 = new Autor(db);
				a1.setNombre("Carlos");
				a1.setApellidos("Ruiz Zafón");
				a1.setEdad(55);
				System.out.println("Autor "+a1.getNombre()+" "+a1.getApellidos()+" creado.");

				// Crea un objeto Libro
				Libro l1 = new Libro(db);
				l1.setTitulo("La sombra del viento");
				l1.setEditorial("Planeta");
				l1.setPaginas(572);
				System.out.println("Libro 'La sombra del viento' creado.");

				// Crea un objeto Libro
				Libro l2 = new Libro(db);
				l2.setTitulo("El juego del ángel");
				l2.setEditorial("Planeta");
				l2.setPaginas(672);
				System.out.println("Libro 'El juego del ángel' creado.");

				// Crea un objeto Libro
				Libro l3 = new Libro(db);
				l3.setTitulo("El prisionero del cielo");
				l3.setEditorial("Planeta");
				l3.setPaginas(384);
				System.out.println("Libro 'El prisionero del cielo' creado.");

				// Crea un objeto Libro
				Libro l4 = new Libro(db);
				l4.setTitulo("El laberinto de los espíritus");
				l4.setEditorial("Planeta");
				l4.setPaginas(928);
				System.out.println("Libro 'El laberinto de los espíritus' creado.");

				// Crea un array de Obras para guardar los libros y hacer las relaciones
				Obra o1[] = new Obra[4];
				o1[0] = l1;
				o1[1] = l2;
				o1[2] = l3;
				o1[3] = l4;

				// Guarda las relaciones del autor con los libros que ha escrito
				a1.setEscribe(o1);

				// Ejecuta un commit para materializar las peticiones
				db.commit();

				// Cierra la base de datos
				db.close();

				System.out.println("\nRealizado.");
			} catch(MtException mte) {
				System.out.println("MtException: "+ mte.getMessage());
			}
		}

		// Método para realizar una Consulta en la base de datos
		public static void ejecutaOQL(String hostname, String dbname) {
			MtDatabase dbcon = new MtDatabase(hostname, dbname);
			// Abre una conexión a la base de datos
			dbcon.open();
			try {
				// Crea una instancia de Statement
				Statement stmt = dbcon.createStatement();

				// Asigna una consulta OQL. Esta consulta lo que hace es utilizar REF() para obtener el objeto directamente en vez de obtener valores concretos
				String commandText = "SELECT REF(a) from biblioteca.Autor a;";

				// Ejecuta la consulta y obtines un ResultSet
				ResultSet rset = stmt.executeQuery(commandText);
				Autor a1;

				// Lee rset uno a uno
				while (rset.next()) {
					// Obtiene los objetos Autor
					a1 = (Autor) rset.getObject(1);
					
					// Imprime los atributos de cada objeto con un formato determinado
					System.out.println("Autor: " + String.format("%16s",  a1.getNombre()) + String.format("%16s",  a1.getApellidos()) + " Spouse: " + String.format("%16s", a1.getEdad()));
				}
				// Cierra las conexiones
				rset.close();
				stmt.close();
			} catch (SQLException e) {
				System.out.println("SQLException: " + e.getMessage());
			}
		}

		// Método para modificar objetos en la base de datos
		public static void modificaObjetos(String hostname, String dbname, String nombre, Integer nuevaEdad) {
			System.out.println("=================== MODIFICA UN OBJETO ==================\n");
			int nAutores = 0;
			try {
				// Abre la BD con el Hostname (localhost), dbname (biblioteca) y el namespace "biblioteca"
				MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
				db.open();
				db.startTransaction();

				// Lista cuántos objetos Autor con el método getInstanceNumber
				System.out.println("\n" + Autor.getInstanceNumber(db) + " Autores en la base de datos.");
				nAutores = (int) Autor.getInstanceNumber(db);

				// Crea un Iterador (propio de Java)
				MtObjectIterator<Autor> iter = Autor.<Autor>instanceIterator(db);
				System.out.println("Recorro el iterador de uno en uno y cambio cuando encuentro 'nombre'");
				while (iter.hasNext()) {
					Autor[] autores = iter.next(nAutores);
					for (int i = 0; i < autores.length; i++) {
						//Busca un autor con nombre 'nombre'
						if (autores[i].getNombre().compareTo(nombre) == 0) {
							autores[i].setEdad(nuevaEdad);
						} else {
						}
					}
				}
				iter.close();
				// Materializa los cambios y cierra la base de datos
				db.commit();
				db.close();

				System.out.println("\nRealizado.");
			} catch(MtException mte) {
				System.out.println("MtException: " + mte.getMessage());
			}
		}

		// Método para Eliminar objetos en la base de datos
		public static void borrarTodos(String hostname, String dbname) {
			System.out.println("===================== BORRAR TODOS ======================\n");
			try {
				// Abre la BD con el Hostname (localhost), dbname (biblioteca) y el namespace "biblioteca"
				MtDatabase db = new MtDatabase(hostname, dbname, new MtPackageObjectFactory("", "biblioteca"));
				db.open();
				db.startTransaction();

				// Lista todos los objetos Obra que hay en la base de datos con el método getInstanceNumber
				System.out.println("\n" + Obra.getInstanceNumber(db) + "Obra(s) en la base de datos.");

				// BORRA todas las instancias de Obra
				Obra.getClass(db).removeAllInstances();

				// Materializa los cambios y cierra la BD
				db.commit();
				db.close();

				System.out.println("\nRealizado.");
			} catch(MtException mte) {
				System.out.println("MtException : " + mte.getMessage());
			}
		}

		static String hostname = "localhost";
		static String dbname = "biblioteca";

		public static void main(String[] args) {
			creaObjetos(hostname, dbname);
			borrarTodos(hostname, dbname);
			modificaObjetos(hostname, dbname, "Carlos", 58);
			ejecutaOQL(hostname, dbname);
		}

}
