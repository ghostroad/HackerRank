
import scala.collection.mutable
import scala.io.Source

class GridCell(val row: Int,
               val col: Int,
               weight: Int) extends Node(weight)

case class Edge(weight: Int, dest: Node)

class Node(val weight: Int,
           var edges: Array[Edge] = Array(),
           var distance: Int = Int.MaxValue,
           var visited: Boolean = false,
           var queued: Boolean = false)

object Node {
  implicit val orderByDistance: Ordering[Node] = Ordering.by[Node, Int](_.distance).reverse
}

case class GridSequence(subgrids: Array[WeightedGrid]) {
  def solution(source: (Int, Int), dest: (Int, Int)): Int = ???
}

object GridSequence {

  val subgridWidth = 100

  def fromWeights(weights: Array[Array[Int]]): GridSequence = {
    val cols = weights.head.length

    GridSequence((0 until cols by subgridWidth - 1).toArray.map { start =>
      WeightedGrid.fromWeights(weights.map(_.slice(start, start + subgridWidth)))
    })
  }
}

object Dijkstra {
  def computeShortestPaths(source: Node, targets: Array[Node]): Unit = {
    val targetSet = mutable.Set(targets:_*)

    source.distance = 0
    val queue = mutable.PriorityQueue[Node](source)
    queue.enqueue()

    while (queue.nonEmpty && targetSet.nonEmpty) {
      val curr = queue.dequeue()

      for (edge <- curr.edges if !edge.dest.visited) {
        val newDistance = edge.weight + curr.distance
        if (newDistance < edge.dest.distance) {
          edge.dest.distance = newDistance
          targetSet.remove(edge.dest)
        }

        if (!edge.dest.queued) {
          queue.enqueue(edge.dest)
          edge.dest.queued = true
        }
      }

      curr.visited = true
    }
  }
}

class WeightedGrid(val cells: Array[Array[GridCell]]) {

  val rows: Int = cells.length
  val cols: Int = cells.head.length

  for {
    i <- 0 until rows
    j <- 0 until cols
  } cells(i)(j).edges = edges(i, j)

  private def getCell(cell: (Int, Int)): GridCell = cells(cell._1)(cell._2)

  private def edges(row: Int, col: Int ): Array[Edge] = {
    Array[(Int, Int)]((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1)).filter {
      case (adjRow, adjCol) => adjRow >= 0 && adjCol >= 0 && adjRow < rows && adjCol < cols
    }.map { case (i, j) => Edge(cells(i)(j).weight, cells(i)(j)) }
  }

  private def clearState(): Unit = {
    for {
      i <- 0 until rows
      j <- 0 until cols
    } {
      val cell = getCell((i, j))
      cell.distance = Int.MaxValue
      cell.visited = false
      cell.queued = false
    }
  }

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    val sourceCell = getCell(source)
    val destCell = getCell(dest)
    if (source == dest) return sourceCell.weight

    clearState()
    Dijkstra.computeShortestPaths(sourceCell, Array(destCell))

    sourceCell.weight + destCell.distance
  }
}

object WeightedGrid {
  def fromWeights(weights: Array[Array[Int]]): WeightedGrid = {
    val rows = weights.length
    val cols = weights.head.length
    val cellGrid: Array[Array[GridCell]] = (0 until rows).toArray.map { i =>
      (0 until cols).toArray.map { j =>
        new GridCell(row = i, col = j, weight = weights(i)(j))
      }
    }
    new WeightedGrid(cellGrid)
  }
}

object ShortestPath extends App {

  def check(inputFileName: String, expectedFileName: String): Unit = {
    val inputFile = Source.fromResource(inputFileName)
    val expectedFile = Source.fromResource(expectedFileName)

    val inputLines = inputFile.getLines()
    val expectedLines = expectedFile.getLines()

    val weightsRows: Int = inputLines.next().split(" ")(0).toInt
    val weights: Array[Array[Int]] = (1 to weightsRows).toArray.map(_ => inputLines.next().split(" ").map(_.toInt))

    val numQueries = inputLines.next().toInt

    val shortestPath = WeightedGrid.fromWeights(weights)

    (1 to numQueries).foreach { i =>
      if (i % 500 == 0) println(i)
      val query = inputLines.next().split(" ").map(_.toInt)
      val expectedOutput = expectedLines.next().toInt
      val actualSolution = shortestPath.solution((query(0), query(1)), (query(2), query(3)))
      if (actualSolution != expectedOutput) {
        println(s"Oops: $i $query $actualSolution $expectedOutput")
      }
    }

    inputFile.close()
    expectedFile.close()
  }

  check("input00.txt", "output00.txt")
  check("input01.txt", "output01.txt")
  check("input03.txt", "output03.txt")
  check("input06.txt", "output06.txt")
}
