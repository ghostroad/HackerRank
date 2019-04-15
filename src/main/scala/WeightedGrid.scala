
import SectionedGrid.SECTION_WIDTH

import scala.io.Source
import scala.math.min

object RunCounter {
  var count: Int = 0
}

class GridCell(val row: Int,
               val col: Int,
               val weight: Int,
               val leftBoundaryDistances: Array[Int],
               val rightBoundaryDistances: Array[Int]) extends Node[GridCell]

object GridCell {
  implicit val cellQueue: MinHeap[GridCell] = new MinHeap[GridCell](1000)
}

case class Edge[T](weight: Int, dest: T)

class MetaNode extends Node[MetaNode]

object MetaNode {
  implicit val metaNodeQueue: MinHeap[MetaNode] = new MinHeap[MetaNode](1000)
}

class Node[T](var edges: Array[Edge[T]] = Array[Edge[T]](),
              var distance: Int = Int.MaxValue,
              var visited: Boolean = false,
              var queued: Boolean = false,
              var queueIndex: Int = -1,
              var previous: Option[T] = None,
              var runNumber: Int = 0
             )

class MinHeap[T <: Node[T]](val size: Int) {

  val heap: Array[Option[T]] = Array.fill[Option[T]](size)(None)
  var end: Int = 1

  def empty: Boolean = end == 1

  def enqueue(elem: T): Unit = {
    storeElement(elem, end)
    bubbleUp(elem)
    end += 1
  }

  def dequeue(): T = {
    val result = heap(1).get

    val lastElement = heap(end - 1).get
    heap(end - 1) = None
    end -= 1
    storeElement(lastElement, 1)

    bubbleDown(lastElement)
    result
  }

  def bubbleUp(elem: T): Unit = {
    while (elem.queueIndex > 1 && parent(elem).distance >= elem.distance) {
      swapWithParent(elem)
    }
  }

  def clear(): Unit = {
    end = 1
  }

  private def bubbleDown(elem: T): Unit = {
    while (leastChild(elem).nonEmpty && leastChild(elem).get.distance < elem.distance) {
      swap(elem, leastChild(elem).get)
    }
  }

  private def leastChild(elem: T): Option[T] = {
    val index = elem.queueIndex
    val leftChildIndex = 2 * index
    val rightChildIndex = 2 * index + 1

    if (leftChildIndex >= end) return None
    if (leftChildIndex == end - 1) return heap(leftChildIndex)

    val leftChild = heap(leftChildIndex)
    val rightChild = heap(rightChildIndex)

    if (leftChild.get.distance <= rightChild.get.distance) leftChild else rightChild
  }

  private def storeElement(elem: T, index: Int): Unit = {
    heap(index) = Some(elem)
    elem.queueIndex = index
  }

  private def parent(elem: T): T = heap(elem.queueIndex / 2).get

  private def swap(a: T, b: T): Unit = {
    val aIndex = a.queueIndex
    val bIndex = b.queueIndex
    storeElement(a, bIndex)
    storeElement(b, aIndex)
  }

  private def swapWithParent(elem: T): Unit = swap(elem, parent(elem))

}

case class SectionedGrid(sections: Array[Section]) extends Solver {

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {

    val sourceSection = sections(source._2 / SECTION_WIDTH)
    val destSection = sections(dest._2 / SECTION_WIDTH)

    val sourceNode = new MetaNode()
    val sourceCell = sourceSection.cellAt(source._1, source._2 % SECTION_WIDTH)

    val destNode = new MetaNode()
    val destCell = destSection.cellAt(dest._1, dest._2 % SECTION_WIDTH)

    sourceNode.edges = sourceSection.leftBoundary.indices.toArray.map { i =>
      if (sourceCell.leftBoundaryDistances(i) < 0) sourceSection.computeShortestPaths(sourceCell)
      Edge[MetaNode](sourceCell.leftBoundaryDistances(i), sourceSection.leftBoundary(i))
    } ++
      sourceSection.rightBoundary.indices.toArray.map { i =>
        if (sourceCell.rightBoundaryDistances(i) < 0) sourceSection.computeShortestPaths(sourceCell)
        Edge[MetaNode](sourceCell.rightBoundaryDistances(i), sourceSection.rightBoundary(i))
      }

    if (sourceSection == destSection) {
      Dijkstra.computeShortestPaths(sourceCell, Some(destCell))
      sourceNode.edges :+= Edge(destCell.distance, destNode)
    }

    destSection.leftBoundary.indices.foreach { i =>
      if (destCell.leftBoundaryDistances(i) < 0) destSection.computeShortestPaths(destCell)
      val neighbor = destSection.leftmostCellAt(i)
      destSection.leftBoundary(i).edges :+= Edge(destCell.leftBoundaryDistances(i) - neighbor.weight + destCell.weight, destNode)
    }

    destSection.rightBoundary.indices.foreach { i =>
      if (destCell.rightBoundaryDistances(i) < 0) destSection.computeShortestPaths(destCell)
      val neighbor = destSection.rightmostCellAt(i)
      destSection.rightBoundary(i).edges :+= Edge(destCell.rightBoundaryDistances(i) - neighbor.weight + destCell.weight, destNode)
    }

    Dijkstra.computeShortestPaths(sourceNode, Some(destNode))

    (destSection.leftBoundary ++ destSection.rightBoundary).foreach { neighbor =>
      neighbor.edges = neighbor.edges.dropRight(1)
    }

    sourceCell.weight + destNode.distance
  }

}


case class Section(leftBoundary: Array[MetaNode],
                   rightBoundary: Array[MetaNode],
                   grid: WeightedGrid) {
  def cellsAtColumn(col: Int): Array[GridCell] = grid.cellsAtColumn(col)

  def cellAt(row: Int, col: Int): GridCell = grid.cells(row)(col)

  def rightmostCellAt(row: Int): GridCell = grid.cells(row)(grid.cols - 1)

  def leftmostCellAt(row: Int): GridCell = grid.cells(row)(0)

  def boundaryCells: Array[GridCell] = grid.cellsAtColumn(0) ++ grid.cellsAtColumn(grid.cols - 1)

  def computeShortestPaths(source: GridCell): Unit = {
    Dijkstra.computeShortestPaths(source)

    grid.cellsAtColumn(0).foreach { leftCell =>
      val distance = leftCell.distance
      var currentCell: Option[GridCell] = Some(leftCell)
      while (currentCell.isDefined) {
        val cell = currentCell.get
        cell.leftBoundaryDistances(leftCell.row) = distance - cell.distance
        currentCell = cell.previous
      }
    }

    grid.cellsAtColumn(grid.cols - 1).foreach { rightCell =>
      val distance = rightCell.distance
      var currentCell: Option[GridCell] = Some(rightCell)
      while (currentCell.isDefined) {
        val cell = currentCell.get
        cell.rightBoundaryDistances(rightCell.row) = distance - cell.distance
        currentCell = cell.previous
      }
    }

  }
}

object SectionedGrid {
  val SECTION_WIDTH = 75

  def fromWeights(weights: Array[Array[Int]]): SectionedGrid = {
    val t0 = System.nanoTime()
    val rows = weights.length
    val cols = weights.head.length

    val grids = (0 until cols by SECTION_WIDTH).toArray.map { start =>
      WeightedGrid.fromWeights(weights.map(_.slice(start, start + SECTION_WIDTH + 1)))
    }

    val boundaries = Array[MetaNode]() +: Array.fill(grids.length - 1)(Array.fill(rows)(new MetaNode())) :+ Array[MetaNode]()

    val sections = boundaries.sliding(2).toArray.zip(grids).map { case (boundaryPair, grid) =>
      Section(boundaryPair(0), boundaryPair(1), grid)
    }

    precomputeEdges(sections)

    val t1 = System.nanoTime()
    println(s"Initialized meta grid in ${(t1 - t0) / 1000000000.0} s.")
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

  def resetNode[T <: Node[T]](currentRunNumber: Int, node: T): Unit = {
    node.runNumber = currentRunNumber
    node.visited = false
    node.queued = false
    node.distance = Int.MaxValue
    node.previous = None
  }

  def computeShortestPaths[T <: Node[T]](source: T, dest: Option[T] = None)(implicit queue: MinHeap[T]): Unit = {
    queue.clear()
    val currentRunNumber = RunCounter.count
    RunCounter.count += 1
    resetNode(currentRunNumber, source)

    source.distance = 0
    queue.enqueue(source)

    while (!queue.empty) {
      val curr = queue.dequeue()
      if (!curr.visited) {
        for (edge <- curr.edges) {
          if (edge.dest.runNumber != currentRunNumber) resetNode(currentRunNumber, edge.dest)
          if (!edge.dest.visited) {
            val newDistance = edge.weight + curr.distance

            if (newDistance < edge.dest.distance) {
              edge.dest.distance = newDistance
              edge.dest.previous = Some(curr)
              if (edge.dest.queued) queue.bubbleUp(edge.dest)
            }

            if (!edge.dest.queued) {
              queue.enqueue(edge.dest)
              edge.dest.queued = true
            }
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

  private def edges(row: Int, col: Int): Array[Edge[GridCell]] = {
    Array[(Int, Int)]((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1)).filter {
      case (adjRow, adjCol) => adjRow >= 0 && adjCol >= 0 && adjRow < rows && adjCol < cols
    }.map { case (i, j) => Edge(cells(i)(j).weight, cells(i)(j)) }
  }

  def cellsAtColumn(col: Int): Array[GridCell] = (0 until rows).toArray.map(cells(_)(col))

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    val sourceCell = getCell(source)
    val destCell = getCell(dest)

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
        new GridCell(row = i, col = j, weight = weights(i)(j), Array.fill(rows)(-1), Array.fill(rows)(-1))
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
      if (i % 5000 == 0) println(i)
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

    println(s"${(t1 - t0) / 1000000000.0} s elapsed.")
  }

  check("input00.txt", "output00.txt")
  check("input01.txt", "output01.txt")
  check("input03.txt", "output03.txt")
  check("input06.txt", "output06.txt")
}
