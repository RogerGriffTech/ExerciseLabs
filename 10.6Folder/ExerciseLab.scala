
object ExercisesLab extends App {

 /* Task 1 - Collections and Transformation Thinking
  • Create a List of employee salaries and use map to apply a 10% increase.
  • Filter the updated list to retain salaries greater than 80,000.
  • Create a Set of departments and demonstrate how duplicate values are handled.
  • Create a Map of employee ID -> department and retrieve values safely.
  Deliverable: Scala source file and console output.
  */
  val salaries = List(20000, 60000, 40000, 82000, 100000)
  val newSalaries = salaries.map(x=> x*1.1)
  println(s"newSalaries is: $newSalaries")
  val filtered_salaries = newSalaries.filter(_ > 80000)
  println(s"filtered_salaries is: $filtered_salaries")
  val departments = Set("Engineering", "Finance", "HR", "Payroll", "HR") //Similar to python, you can't have duplicates in a Set.
  println(s"departments is: $departments")
  val emp = Map(
    "employee_id" -> "department"
  )
  println(emp("employee_id"))


  /*
  Task 2 - Pattern Matching
    • Given departments Engineering, HR, Finance, Legal and Sales, classify each into a business category using pattern matching.
    • Use a default case for an unknown department.
    • Apply the classification to a collection using map.
Deliverable: Output showing original department and derived category.
   */
  val deps = List("Engineering", "HR", "Finance", "Legal", "Sales")
  val cat_deps = deps.map(departments => {
    departments match{
      case "Engineering" | "Finance" => "Math"
      case "Legal" => "Satanic Studies"
      case "HR" | "Sales" => "Business"
      case _ => "Unknown"
    }
  }
  )
  println(s"cat_deps is: $cat_deps")

/*
Task 3 - Null Safety with Option
    • Create salary data containing Some(value) and None.
    • Replace missing salary with 0 using getOrElse.
    • Explain in 3-4 lines why Option is preferable to null in data pipelines.
Deliverable: Code, output, and explanation.
 */

  val salary = List(Some(90000), None, None, Some(70000), Some(65000), Some(20000), None)
  println(s"salary is: $salary")
  val updated_salary = salary.map(_.getOrElse(0))
  println(s"updated salary is: $updated_salary")
  // you want to use option because its type-safe and requires you to handle whether or not the values exists.

  /*
  Task 4 - Reusable Validation with Traits
    • Create a Validator trait with validateSalary(salary:Int): Boolean.
    • Create an EmployeeValidator class that uses the trait.
    • Test positive, zero and negative salary values.
Deliverable: Trait/class implementation and test output.
   */
trait Validator {
    def validateSalary(salary:Int): Boolean
  }
class EmployeeValidator extends Validator {
  override def validateSalary(salary: Int): Boolean = {
  if(salary > 0) true else false
  }
  }
val employ = new EmployeeValidator()
println(employ.validateSalary(100000))
  println(employ.validateSalary(0))
  println(employ.validateSalary(-200))


/*
Task 5 - Safe Parsing with Try
    • Parse List("10","20","abc","40","not_available") into integers without terminating the program.
    • Separate valid values from invalid values or apply a documented fallback.
    • Print the cleaned collection and the count of invalid inputs.
Deliverable: Code and output.
Challenge: Do not use an unsafe direct .toInt on untrusted input.
 */
  var parse_list = List("10","20","abc","40","not_available")
  var count = 0
//parse_list.map(value => try{value.toInt}
//  catch {
//    case n: NumberFormatException => count += 1
//  }
//  )
  import scala.util.Try
  import scala.collection.mutable.ListBuffer
  val buf = ListBuffer[String]()
 val success_list = parse_list.map(value => try{value.toInt}
  catch{
    case n: NumberFormatException => {
      count += 1

    }
  })
println(success_list)
//  import scala.util.Try
//  val parsed_success = parse_list.map(value => Try{toInt(value).isSuccess()})
//  println(parsed_success)




  /*
  Task 6 - Mini Pipeline
    • Create Employee(id, name, dept, salary) as a case class.
    • Clean missing/invalid input, filter salary > 70,000, group by department, and calculate employee count, total salary and average salary.
    • Keep transformations readable as a pipeline rather than one large imperative block.
Deliverable: scala_fundamentals_exercise.scala + output.
   */
case class Employee(id: Int, name: String, dept: String, salary: Float)










}
