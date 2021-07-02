import org.junit._
import org.junit.Assert._


class UnitTestBigDataJUnit {

  @Test   //annotation qui indique que la fonction qui suit est un test
  def testDivision () : Unit = {
    var valeur_actuelle : Double = HelloWorldBigData.division(10, 2)
    var valeur_prevue : Int = 5
    assertEquals("résultat attendu : la fonction doit renvoyer normalement 5", valeur_prevue, valeur_actuelle.toInt)
  }

  @Test
  def testConversion () : Unit = {
    var valeur_actuelle : Int = HelloWorldBigData.convert_entier("15")
    var valeur_prevue : Int = 15
    assertSame("résultat attendu : la fonction doit renvoyer normalement le nombre", valeur_prevue, valeur_actuelle)
  }

  @Test
  def testComptageCaractere () : Unit = {
    var valeur_actuelle : Int = HelloWorldBigData.comptage_caracteres("exemple de caractères")
    var valeur_prevue : Int = 21
    assertSame("résultat attendu : la fonction doit renvoyer normalement le nombre", valeur_prevue, valeur_actuelle)
  }


}
