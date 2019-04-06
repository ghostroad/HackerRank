
import SectionedGrid.SECTION_WIDTH

import scala.collection.mutable
import scala.io.Source


class GridCell(val row: Int,
               val col: Int,
               val weight: Int) extends Node[GridCell]

class MetaNode(val cell: GridCell) extends Node[MetaNode]

case class Edge[T <: Node[T]](weight: Int, dest: T)

class Node[T](var edges: Array[Edge[T]] = Array(),
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
  implicit def orderByDistance[T]: Ordering[Node[T]] = Ordering.by[Node[T], Int](_.distance).reverse
}

case class SectionedGrid(grid: WeightedGrid, boundaryLocations: Array[Int], boundaries: Array[Array[MetaNode]]) extends Solver {

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    if (source._2 / SECTION_WIDTH == dest._2 / SECTION_WIDTH) {
      return grid.solution(source, dest)
    }

    clearMetaGraph()

    val sourceNode = createSourceNode(source)
    val (destinationNode, destinationNeighbors) = createDestinationNode(dest)
    Dijkstra.storeShortestPaths(sourceNode, Array(destinationNode))

    destinationNeighbors.foreach { neighbor =>
      neighbor.edges = neighbor.edges.dropRight(1)
    }

    sourceNode.cell.weight + destinationNode.distance
  }

  private def createSourceNode(source: (Int, Int)) = {
    val i = source._2 / SECTION_WIDTH
    val sourceNode = new MetaNode(grid.cells(source._1)(source._2))

    grid.clearState()
    val left = if (i > 0) boundaries(i - 1) else Array[MetaNode]()
    val right = if (i < boundaries.length) boundaries(i) else Array[MetaNode]()
    val neighbors = left ++ right
    Dijkstra.storeShortestPaths(sourceNode.cell, neighbors.map(_.cell))

    sourceNode.edges = neighbors.map(neighbor => Edge(neighbor.cell.distance, neighbor))
    sourceNode
  }

  private def createDestinationNode(dest: (Int, Int)): (MetaNode, Array[MetaNode]) = {
    val i = dest._2 / SECTION_WIDTH
    val destCell = grid.cells(dest._1)(dest._2)
    val destMetaNode = new MetaNode(destCell)

    grid.clearState()
    val left = if (i > 0) boundaries(i - 1) else Array[MetaNode]()
    val right = if (i < boundaries.length) boundaries(i) else Array[MetaNode]()
    val neighbors = left ++ right
    Dijkstra.storeShortestPaths(destCell, neighbors.map(_.cell))

    neighbors.foreach { neighbor =>
      neighbor.edges = neighbor.edges :+ Edge(neighbor.cell.distance - neighbor.cell.weight + destCell.weight, destMetaNode)
    }

    (destMetaNode, neighbors)
  }

  def clearMetaGraph(): Unit = {
    boundaries.foreach(_.foreach(_.clear()))
  }

  override def clearingTime: Long = grid.clearingTime
}

object FloydWarshall {

  def computeDistances(grid: WeightedGrid): Array[Array[Array[Array[Int]]]] = {
    val distances = Array.fill(grid.rows, grid.cols, grid.rows, grid.cols)(Int.MaxValue)
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
                   start: Int,
                   end: Int)

object SectionedGrid {
  val SECTION_WIDTH = 100

  def fromWeights(weights: Array[Array[Int]]): SectionedGrid = {
    val t0 = System.nanoTime()
    val rows = weights.length
    val cols = weights.head.length
    val sections = (0 until cols by SECTION_WIDTH).map { start =>
      val grid = WeightedGrid.fromWeights(weights.map(_.slice(start, start + SECTION_WIDTH)))
      val distanceCache = FloydWarshall.computeDistances(grid)
      val leftBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(0))}
      val rightBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(grid.cols - 1))}

      val allNodes = leftBoundary ++ rightBoundary
      allNodes.foreach { node =>
        val neighbors = allNodes.filter(_ != node)
        node.edges = neighbors.map(neighbor => Edge(distanceCache(node.cell.row)(node.cell.col)(neighbor.cell.row)(neighbor.cell.col), neighbor))
      }
      Section(leftBoundary, rightBoundary, distanceCache, start, start + SECTION_WIDTH - 1)
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

    val grid = WeightedGrid.fromWeights(weights)

    val boundaryLocations = (SECTION_WIDTH - 1 until grid.cols by SECTION_WIDTH).toArray

    assert(boundaryLocations.length > 1)

    val boundaries: Array[Array[MetaNode]] = boundaryLocations.map { col =>
      (0 until grid.rows).toArray.map { row =>
        new MetaNode(grid.cells(row)(col))
      }
    }

    boundaries.indices.foreach { i =>
      val boundary = boundaries(i)
      boundary.foreach { node =>
        grid.clearState()
        val siblings = boundary.filter(_ != node)
        val left = if (i > 0) boundaries(i - 1) else Array[MetaNode]()
        val right = if (i < boundaries.length - 1) boundaries(i + 1) else Array[MetaNode]()
        val neighbors = siblings ++ left ++ right
        Dijkstra.storeShortestPaths(node.cell, neighbors.map(_.cell))

        node.edges = neighbors.map(neighbor => Edge(neighbor.cell.distance, neighbor))
      }
    }

    val t1 = System.nanoTime()
    println(s"Initialized meta grid in ${(t1 - t0)/1000000000.0} s.")
    SectionedGrid(grid, boundaryLocations, boundaries)
  }
}

object Dijkstra {
  def storeShortestPaths[T <: Node[T]](source: Node[T], targets: Array[Node[T]]): Unit = {
    val targetSet = mutable.Set(targets:_*)

    source.distance = 0
    targetSet.remove(source)

    val queue = mutable.PriorityQueue[Node[T]](source)
    queue.enqueue()

    while (queue.nonEmpty && targetSet.nonEmpty) {
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
      targetSet.remove(curr)
    }
  }
}

trait Solver {
  def solution(source: (Int, Int), dest: (Int, Int)): Int

  def clearingTime: Long
}

class WeightedGrid(val cells: Array[Array[GridCell]]) extends Solver {

  val rows: Int = cells.length
  val cols: Int = cells.head.length
  var clearingTime: Long = 0

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
    val t0 = System.nanoTime()
    cells.foreach(_.foreach(_.clear()))
    clearingTime += System.nanoTime() - t0
  }

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    val sourceCell = getCell(source)
    val destCell = getCell(dest)

    clearState()
    Dijkstra.storeShortestPaths(sourceCell, Array(destCell))

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

    (1 to numQueries).foreach { i =>
      if (i % 500 == 0) println(i)
      val query = inputLines.next().split(" ").map(_.toInt)
      val expectedOutput = expectedLines.next().toInt
      val actualSolution = solver.solution((query(0), query(1)), (query(2), query(3)))
      if (actualSolution != expectedOutput) {
        println(s"Oops: $i ${query.mkString(",")} $actualSolution $expectedOutput")
      }
    }

    inputFile.close()
    expectedFile.close()

    val t1 = System.nanoTime()

    println(s"${(t1 - t0)/1000000000.0} s elapsed.")
    println(s"Spent ${solver.clearingTime/1000000000.0} s clearing grid.")
  }

  check("input00.txt", "output00.txt")
  check("input01.txt", "output01.txt")
  check("input03.txt", "output03.txt")
  check("input06.txt", "output06.txt")
}
