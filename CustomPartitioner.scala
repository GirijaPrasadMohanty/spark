package sparkUtilityCodes
import org.apache.spark.Partitioner

class CustomPartitioner(override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
		case s: String => {
			if (s(0).toUpper == 'A')  1 
			else if (s(0).toUpper == 'B')  2
			else if (s(0).toUpper == 'C')  3
			else if (s(0).toUpper == 'D')  4
			else if (s(0).toUpper == 'E') 5
			else if (s(0).toUpper == 'F')  6
			else if (s(0).toUpper == 'G')  7
			else if (s(0).toUpper == 'H')  8
			else if (s(0).toUpper == 'I')  9
			else if (s(0).toUpper == 'J')  10
			else if (s(0).toUpper == 'K')  11
			else if (s(0).toUpper == 'L')  12
			else if (s(0).toUpper == 'M')  13
			else if (s(0).toUpper == 'N')  14
			else if (s(0).toUpper == 'O')  15
			else if (s(0).toUpper == 'P')  16
			else if (s(0).toUpper == 'Q')  17
			else if (s(0).toUpper == 'R')  18
			else if (s(0).toUpper == 'S')  19
			else if (s(0).toUpper == 'T')  20
			else if (s(0).toUpper == 'U')  21
			else if (s(0).toUpper == 'V')  22
			else if (s(0).toUpper == 'W')  23
			else if (s(0).toUpper == 'X')  24
			else if (s(0).toUpper == 'Y')  25
			else  26
		}   
	}
}
