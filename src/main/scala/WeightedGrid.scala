
import SectionedGrid.SECTION_WIDTH

import scala.collection.mutable
import scala.io.Source


class GridCell(val row: Int,
               val col: Int,
               val weight: Int) extends Node[GridCell]

class MetaNode(val cell: GridCell) extends Node[MetaNode]

case class Edge[T <: Node[T]](weight: Int, dest: T)

class Node[T <: Node[T]](var edges: Array[Edge[T]] = Array[Edge[T]](),
           var distance: Int = Int.MaxValue,
           var visited: Boolean = false,
           var queued: Boolean = false) {

  def clear(): Unit = {
    distance = Int.MaxValue
    visited = false
    queued = false
  }
}

object Node {
  implicit def orderByDistance[T <: Node[T]]: Ordering[Node[T]] = Ordering.by[Node[T], Int](_.distance).reverse
}

case class SectionedGrid(sections: Array[Section]) extends Solver {

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    clearMetaGraph()

    val sourceSection = sections(source._2 / SECTION_WIDTH)
    val sourceNode = new MetaNode(sourceSection.grid.cells(source._1)(source._2 % SECTION_WIDTH))

    sourceNode.edges = (sourceSection.leftBoundary ++ sourceSection.rightBoundary).map { neighbor =>
      Edge(sourceSection.distance(sourceNode.cell, neighbor.cell), neighbor)
    }


    val destSection = sections(dest._2 / SECTION_WIDTH)
    val destNode = new MetaNode(destSection.grid.cells(dest._1)(dest._2 % SECTION_WIDTH))

    val destinationNeighbors = destSection.leftBoundary ++ destSection.rightBoundary

    destinationNeighbors.foreach { neighbor =>
      neighbor.edges :+= Edge(destSection.distance(neighbor.cell, destNode.cell), destNode)
    }

    if (destSection == sourceSection) {
      sourceNode.edges :+= Edge(destSection.distance(sourceNode.cell, destNode.cell), destNode)
    }

    val result = Dijkstra.shortestPath(sourceNode, destNode)

    destinationNeighbors.foreach { neighbor =>
      neighbor.edges = neighbor.edges.dropRight(1)
    }

    sourceNode.cell.weight + result
  }

  def clearMetaGraph(): Unit = {
    sections.foreach(_.clear())
  }

}

object FloydWarshall {

  def computeDistances(grid: WeightedGrid): Array[Array[Array[Array[Int]]]] = {
    val distances = Array.fill(grid.rows, grid.cols, grid.rows, grid.cols)(5000000)
    val allCells = grid.allCells
    allCells.foreach { cell =>
      distances(cell.row)(cell.col)(cell.row)(cell.col) = 0
      cell.edges.foreach { edge =>
          distances(cell.row)(cell.col)(edge.dest.row)(edge.dest.col) = edge.weight
        }
      }

    for {
      k <- allCells
      i <- allCells
      j <- allCells
    } {
      if (distances(i.row)(i.col)(j.row)(j.col) > distances(i.row)(i.col)(k.row)(k.col) + distances(k.row)(k.col)(j.row)(j.col)) {
        distances(i.row)(i.col)(j.row)(j.col) = distances(i.row)(i.col)(k.row)(k.col) + distances(k.row)(k.col)(j.row)(j.col)
      }
    }

    distances

  }

}

case class Section(leftBoundary: Array[MetaNode],
                   rightBoundary:Array[MetaNode],
                   distanceCache: Array[Array[Array[Array[Int]]]],
                   grid: WeightedGrid,
                   start: Int,
                   end: Int) {
  def clear(): Unit = {
    leftBoundary.foreach(_.clear())
    rightBoundary.foreach(_.clear())
  }

  def distance(sourceCell: GridCell, destCell: GridCell): Int = distanceCache(sourceCell.row)(sourceCell.col)(destCell.row)(destCell.col)
}

object SectionedGrid {
  val SECTION_WIDTH = 50

  def fromWeights(weights: Array[Array[Int]]): SectionedGrid = {
    val t0 = System.nanoTime()
    val rows = weights.length
    val cols = weights.head.length
    val sections = (0 until cols by SECTION_WIDTH).toArray.map { start =>
      println(start)
      val grid = WeightedGrid.fromWeights(weights.map(_.slice(start, start + SECTION_WIDTH)))
      val distanceCache = FloydWarshall.computeDistances(grid)
      val leftBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(0))}
      val rightBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(grid.cols - 1))}

      val allNodes = leftBoundary ++ rightBoundary
      allNodes.foreach { node =>
        val neighbors = allNodes.filter(_ != node)
        node.edges = neighbors.map(neighbor => Edge(distanceCache(node.cell.row)(node.cell.col)(neighbor.cell.row)(neighbor.cell.col), neighbor))
      }
      Section(leftBoundary, rightBoundary, distanceCache, grid, start, start + SECTION_WIDTH - 1)
    }

    sections.sliding(2).foreach { pair =>
      val leftSection = pair(0)
      val rightSection = pair(1)
      (0 until rows).foreach { row =>
        val leftNode = leftSection.rightBoundary(row)
        val rightNode = rightSection.leftBoundary(row)
        leftNode.edges :+= Edge(weights(row)(rightSection.start), rightNode)
        rightNode.edges :+= Edge(weights(row)(leftSection.end), leftNode)
      }
    }

    val t1 = System.nanoTime()
    println(s"Initialized meta grid in ${(t1 - t0)/1000000000.0} s.")
    SectionedGrid(sections)
  }
}

object Dijkstra {
  def shortestPath[T <: Node[T]](source: Node[T], target: Node[T]): Int = {
    if (source == target) return 0

    source.distance = 0

    val queue = mutable.PriorityQueue[Node[T]](source)
    queue.enqueue()

    while (queue.nonEmpty) {
      val curr = queue.dequeue()

      for (edge <- curr.edges if !edge.dest.visited) {
        val newDistance = edge.weight + curr.distance
        if (newDistance < edge.dest.distance) {
          edge.dest.distance = newDistance
        }

        if (!edge.dest.queued) {
          queue.enqueue(edge.dest)
          edge.dest.queued = true
        }
      }

      curr.visited = true
      if (curr == target) return curr.distance
    }

    throw new IllegalStateException("Shouldn't have gotten here!")
  }
}

trait Solver {
  def solution(source: (Int, Int), dest: (Int, Int)): Int
}

class WeightedGrid(val cells: Array[Array[GridCell]]) extends Solver {

  val rows: Int = cells.length
  val cols: Int = cells.head.length

  for {
    i <- 0 until rows
    j <- 0 until cols
  } cells(i)(j).edges = edges(i, j)

  private def getCell(cell: (Int, Int)): GridCell = cells(cell._1)(cell._2)

  private def edges(row: Int, col: Int ): Array[Edge[GridCell]] = {
    Array[(Int, Int)]((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1)).filter {
      case (adjRow, adjCol) => adjRow >= 0 && adjCol >= 0 && adjRow < rows && adjCol < cols
    }.map { case (i, j) => Edge(cells(i)(j).weight, cells(i)(j)) }
  }

  def allCells: Array[GridCell] = {
    for {
      row <- cells
      cell <- row
    } yield cell
  }

  def clearState(): Unit = {
    cells.foreach(_.foreach(_.clear()))
  }

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    val sourceCell = getCell(source)
    val destCell = getCell(dest)

    clearState()

    sourceCell.weight + Dijkstra.shortestPath(sourceCell, destCell)
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
    val t0 = System.nanoTime()
    println(s"Checking $inputFileName")

    val inputFile = Source.fromResource(inputFileName)
    val expectedFile = Source.fromResource(expectedFileName)

    val inputLines = inputFile.getLines()
    val expectedLines = expectedFile.getLines()

    val dimensions = inputLines.next().split(" ")
    val weightsRows: Int = dimensions(0).toInt
    val weightsCols: Int = dimensions(1).toInt

    val weights: Array[Array[Int]] = (1 to weightsRows).toArray.map(_ => inputLines.next().split(" ").map(_.toInt))

    val numQueries = inputLines.next().toInt

    val solver: Solver =
      if (weightsCols <= 2 * SECTION_WIDTH) {
        WeightedGrid.fromWeights(weights)
      } else {
        SectionedGrid.fromWeights(weights)
      }

    val distanceCache = FloydWarshall.computeDistances(WeightedGrid.fromWeights(weights))

    (1 to numQueries).foreach { i =>
      if (i % 500 == 0) println(i)
      val query = inputLines.next().split(" ").map(_.toInt)
      val expectedOutput = expectedLines.next().toInt
      val actualSolution = solver.solution((query(0), query(1)), (query(2), query(3)))
      val fwSolution = weights(query(0))(query(1)) + distanceCache(query(0))(query(1))(query(2))(query(3))
      if (actualSolution != expectedOutput) {
        println(s"Oops: $i ${query.mkString(",")} $actualSolution $expectedOutput")
      }
      if (fwSolution != expectedOutput) {
        println(s"Oops (fw): $i ${query.mkString(",")} $fwSolution $expectedOutput")
      }
    }

    inputFile.close()
    expectedFile.close()

    val t1 = System.nanoTime()

    println(s"${(t1 - t0)/1000000000.0} s elapsed.")
  }

  check("input00.txt", "output00.txt")
  check("input01.txt", "output01.txt")
//  check("input03.txt", "output03.txt")
//  check("input06.txt", "output06.txt")
}
