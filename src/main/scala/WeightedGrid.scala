
import SectionedGrid.SECTION_WIDTH

import scala.io.Source
import scala.math.min

class GridCell(val row: Int,
               val col: Int,
               val weight: Int) extends Node

trait Queueable {
  var queueIndex: Option[Int]
  def priority: Int
}

case class Edge(weight: Int, dest: Node)

class Node(var edges: Array[Edge] = Array[Edge](),
           var distance: Int = Int.MaxValue,
           var visited: Boolean = false,
           var queued: Boolean = false,
           var queueIndex: Option[Int] = None) extends Queueable {

  def priority: Int = distance

  def clear(): Unit = {
    distance = Int.MaxValue
    visited = false
    queued = false
  }
}

class MinHeap[T <: Queueable](val size: Int) {

  val heap: Array[Option[T]] = Array.fill[Option[T]](size)(None)
  var end: Int = 1

  def empty: Boolean = end == 1

  def enqueue(elem: T): Unit = {
    storeElement(elem, end)
    bubbleUp(elem)
    end += 1
  }

  def dequeue(): T = {
    if (empty) throw new IllegalStateException("Empty!")
    val result = heap(1).get

    val lastElement = heap(end - 1).get
    heap(end - 1) = None
    end -= 1
    storeElement(lastElement, 1)

    bubbleDown(lastElement)
    result
  }

  def bubbleUp(elem: T): Unit = {
    while (elem.queueIndex.get > 1 && parent(elem).priority > elem.priority) {
      swapWithParent(elem)
    }
  }

  def clear(): Unit = {
    for (i <- 1 until end) heap(i) = None
    end = 1
  }

  private def bubbleDown(elem: T): Unit = {
    while (leastChild(elem).nonEmpty && leastChild(elem).get.priority < elem.priority) {
      swap(elem, leastChild(elem).get)
    }
  }

  private def leastChild(elem: T): Option[T] = {
    val index = elem.queueIndex.get
    val maybeLeft = heap(2 * index)
    val maybeRight = heap(2 * index + 1)

    (maybeLeft, maybeRight) match {
      case (Some(left), Some(right)) if left.priority <= right.priority => Some(left)
      case (Some(left), Some(right)) if right.priority < left.priority => Some(right)
      case (Some(left), None) => Some(left)
      case _ => None
    }
  }

  private def storeElement(elem: T, index: Int): Unit = {
    heap(index) = Some(elem)
    elem.queueIndex = Some(index)
  }

  private def parent(elem: T): T = heap(elem.queueIndex.get / 2).get

  private def swap(a: T, b: T): Unit = {
    val aIndex = a.queueIndex.get
    val bIndex = b.queueIndex.get
    storeElement(a, bIndex)
    storeElement(b, aIndex)
  }

  private def swapWithParent(elem: T): Unit = swap(elem, parent(elem))

}

case class SectionedGrid(sections: Array[Section]) extends Solver {

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {

    val sourceSection = sections(source._2 / SECTION_WIDTH)
    val destSection = sections(dest._2 / SECTION_WIDTH)

    val sourceNode = new Node()
    val sourceCell = sourceSection.cellAt(source._1, source._2 % SECTION_WIDTH)

    val destNode = new Node()
    val destCell = destSection.cellAt(dest._1, dest._2 % SECTION_WIDTH)

    sourceSection.computeShortestPaths(sourceCell)

    sourceNode.edges = sourceSection.leftBoundary.indices.toArray.map { i =>
      Edge(sourceSection.leftmostCellAt(i).distance, sourceSection.leftBoundary(i))
    } ++
    sourceSection.rightBoundary.indices.toArray.map { i =>
      Edge(sourceSection.rightmostCellAt(i).distance, sourceSection.rightBoundary(i))
    }

    if (sourceSection == destSection) {
      sourceNode.edges :+= Edge(destCell.distance, destNode)
    }

    destSection.computeShortestPaths(destCell)

    destSection.leftBoundary.indices.foreach { i =>
      val neighbor = destSection.leftmostCellAt(i)
      destSection.leftBoundary(i).edges :+= Edge(neighbor.distance - neighbor.weight + destCell.weight, destNode)
    }

    destSection.rightBoundary.indices.foreach { i =>
      val neighbor = destSection.rightmostCellAt(i)
      destSection.rightBoundary(i).edges :+= Edge(neighbor.distance - neighbor.weight + destCell.weight, destNode)
    }

    clearMetaGraph()
    Dijkstra.computeShortestPaths(sourceNode, Some(destNode))

    (destSection.leftBoundary ++ destSection.rightBoundary).foreach { neighbor =>
      neighbor.edges = neighbor.edges.dropRight(1)
    }

    sourceCell.weight + destNode.distance
  }

  def clearMetaGraph(): Unit = sections.foreach(_.clearBoundaries())

}


case class Section(leftBoundary: Array[Node],
                   rightBoundary: Array[Node],
                   grid: WeightedGrid) {
  def cellsAtColumn(col: Int): Array[Node] = grid.cellsAtColumn(col)

  def clearBoundaries(): Unit = {
    leftBoundary.foreach(_.clear())
    rightBoundary.foreach(_.clear())
  }

  def cellAt(row: Int, col: Int): GridCell = grid.cells(row)(col)

  def rightmostCellAt(row: Int): GridCell = grid.cells(row)(grid.cols - 1)

  def leftmostCellAt(row: Int): GridCell = grid.cells(row)(0)

  def boundaryCells: Array[Node] = grid.cellsAtColumn(0) ++ grid.cellsAtColumn(grid.cols - 1)

  def computeShortestPaths(source: Node): Unit = {
    grid.clearState()
    Dijkstra.computeShortestPaths(source)
  }
}

object SectionedGrid {
  val SECTION_WIDTH = 50

  def fromWeights(weights: Array[Array[Int]]): SectionedGrid = {
    val t0 = System.nanoTime()
    val rows = weights.length
    val cols = weights.head.length

    val grids = (0 until cols by SECTION_WIDTH).toArray.map { start =>
      WeightedGrid.fromWeights(weights.map(_.slice(start, start + SECTION_WIDTH + 1)))
    }

    val boundaries = Array[Node]() +: Array.fill(grids.length - 1)(Array.fill(rows)(new Node())) :+ Array[Node]()

    val sections = boundaries.sliding(2).toArray.zip(grids).map { case (boundaryPair, grid) =>
      Section(boundaryPair(0), boundaryPair(1), grid)
    }

    precomputeEdges(sections)

    val t1 = System.nanoTime()
    println(s"Initialized meta grid in ${(t1 - t0)/1000000000.0} s.")
    SectionedGrid(sections)
  }

  private def precomputeEdges(sections: Array[Section]): Unit = {
    val rows = sections(0).grid.rows

    sections.sliding(2).foreach { sectionPair =>
      val leftSection = sectionPair(0)
      val rightSection = sectionPair(1)

      val thisBoundary = leftSection.rightBoundary
      val previousBoundary = leftSection.leftBoundary

      (0 until rows).foreach { row =>
        val thisCell = leftSection.rightmostCellAt(row)

        leftSection.computeShortestPaths(thisCell)
        rightSection.computeShortestPaths(rightSection.leftmostCellAt(row))

        val siblingEdges = (0 until rows).filter(_ != row).toArray.map { otherRow =>
          Edge(min(leftSection.rightmostCellAt(otherRow).distance, rightSection.leftmostCellAt(otherRow).distance), thisBoundary(otherRow))
        }

        val leftEdges = previousBoundary.indices.toArray.map { i =>
          Edge(leftSection.leftmostCellAt(i).distance, previousBoundary(i))
        }

        thisBoundary(row).edges = siblingEdges ++ leftEdges

        previousBoundary.indices.foreach { i =>
          val cell = leftSection.leftmostCellAt(i)
          previousBoundary(i).edges :+= Edge(cell.distance - cell.weight + thisCell.weight, thisBoundary(row))
        }
      }
    }
  }
}

object Dijkstra {
  val queue = new MinHeap[Node](5000)

  def computeShortestPaths(source: Node, dest: Option[Node] = None): Unit = {
    queue.clear()

    source.distance = 0
    queue.enqueue(source)

    while (!queue.empty) {
      val curr = queue.dequeue()
      if (!curr.visited) {
        for (edge <- curr.edges if !edge.dest.visited) {
          val newDistance = edge.weight + curr.distance

          if (newDistance < edge.dest.distance) {
            edge.dest.distance = newDistance
            if (edge.dest.queued) queue.bubbleUp(edge.dest)
          }

          if (!edge.dest.queued) {
            queue.enqueue(edge.dest)
            edge.dest.queued = true
          }
        }

        curr.visited = true
        if (dest.isDefined && dest.get == curr) return
      }
    }
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

  private def edges(row: Int, col: Int ): Array[Edge] = {
    Array[(Int, Int)]((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1)).filter {
      case (adjRow, adjCol) => adjRow >= 0 && adjCol >= 0 && adjRow < rows && adjCol < cols
    }.map { case (i, j) => Edge(cells(i)(j).weight, cells(i)(j)) }
  }

  def cellsAtColumn(col: Int): Array[Node] = (0 until rows).toArray.map(cells(_)(col))

  def clearState(): Unit = {
    cells.foreach(_.foreach(_.clear()))
  }

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    val sourceCell = getCell(source)
    val destCell = getCell(dest)

    clearState()
    Dijkstra.computeShortestPaths(sourceCell, Some(destCell))

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
      if (i % 1000 == 0) println(i)
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
  }

  check("input00.txt", "output00.txt")
  check("input01.txt", "output01.txt")
  check("input03.txt", "output03.txt")
  check("input06.txt", "output06.txt")
}
