
import SectionedGrid.SECTION_WIDTH

import scala.collection.mutable
import scala.io.Source

class GridCell(val row: Int,
               val col: Int,
               val weight: Int) extends Node

class MetaNode(val cell: GridCell) extends Node

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

  def clear(): Unit = {
    for (i <- 1 until end) {
      heap(i) = None
    }
    end = 1
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

  private def parent(elem: T): T = {
    heap(elem.queueIndex.get / 2).get
  }

  private def swap(a: T, b: T): Unit = {
    val aIndex = a.queueIndex.get
    val bIndex = b.queueIndex.get
    storeElement(a, bIndex)
    storeElement(b, aIndex)
  }

  private def swapWithParent(elem: T): Unit = {
    swap(elem, parent(elem))
  }

  def bubbleUp(elem: T): Unit = {
    while (elem.queueIndex.get > 1 && parent(elem).priority > elem.priority) {
      swapWithParent(elem)
    }
  }


}

object Node {
  implicit def orderByDistance: Ordering[Node] = Ordering.by[Node, Int](_.distance).reverse
}

case class SectionedGrid(sections: Array[Section]) extends Solver {

  def solution(source: (Int, Int), dest: (Int, Int)): Int = {
    clearMetaGraph()

    val sourceSection = sections(source._2 / SECTION_WIDTH)
    val destSection = sections(dest._2 / SECTION_WIDTH)

    val sourceNode = new MetaNode(sourceSection.grid.cells(source._1)(source._2 % SECTION_WIDTH))
    val destNode = new MetaNode(destSection.grid.cells(dest._1)(dest._2 % SECTION_WIDTH))

    sourceSection.clearGrid()
    val sourceNeighbors = sourceSection.leftBoundary ++ sourceSection.rightBoundary ++
      (if (sourceSection == destSection) Array(destNode) else Array[MetaNode]())
    Dijkstra.computeShortestPaths(sourceNode.cell, sourceNeighbors.map(_.cell))

    sourceNode.edges = sourceNeighbors.map { neighbor =>
      Edge(neighbor.cell.distance, neighbor)
    }

    if (sourceSection == destSection) {
      sourceNode.edges :+= Edge(destNode.cell.distance, destNode)
    }

    destSection.clearGrid()
    val destinationNeighbors = destSection.leftBoundary ++ destSection.rightBoundary
    Dijkstra.computeShortestPaths(destNode.cell, destinationNeighbors.map(_.cell))

    destinationNeighbors.foreach { neighbor =>
      neighbor.edges :+= Edge(neighbor.cell.distance - neighbor.cell.weight + destNode.cell.weight, destNode)
    }

    Dijkstra.computeShortestPaths(sourceNode, Array(destNode))

    destinationNeighbors.foreach { neighbor =>
      neighbor.edges = neighbor.edges.dropRight(1)
    }

    sourceNode.cell.weight + destNode.distance
  }

  def clearMetaGraph(): Unit = {
    sections.foreach(_.clearBoundaries())
  }

}


case class Section(leftBoundary: Array[MetaNode],
                   rightBoundary:Array[MetaNode],
                   grid: WeightedGrid,
                   start: Int,
                   end: Int) {
  def clearBoundaries(): Unit = {
    leftBoundary.foreach(_.clear())
    rightBoundary.foreach(_.clear())
  }

  def clearGrid(): Unit = {
    grid.clearState()
  }
}

object SectionedGrid {
  val SECTION_WIDTH = 50

  def fromWeights(weights: Array[Array[Int]]): SectionedGrid = {
    val t0 = System.nanoTime()
    val rows = weights.length
    val cols = weights.head.length
    val sections = (0 until cols by SECTION_WIDTH).toArray.map { start =>
      val grid = WeightedGrid.fromWeights(weights.map(_.slice(start, start + SECTION_WIDTH)))
      val leftBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(0)) }
      val rightBoundary = (0 until grid.rows).toArray.map { row => new MetaNode(grid.cells(row)(grid.cols - 1)) }

      val allNodes = leftBoundary ++ rightBoundary
      allNodes.foreach { node =>
        val neighbors = allNodes.filter(_ != node)
        grid.clearState()
        Dijkstra.computeShortestPaths(node.cell, neighbors.map(_.cell))
        node.edges = neighbors.map(neighbor => Edge(neighbor.cell.distance, neighbor))
      }
      Section(leftBoundary, rightBoundary, grid, start, start + grid.cols - 1)
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
  val queue = new MinHeap[Node](5000)

  def computeShortestPaths(source: Node, target: Array[Node]): Unit = {
    val targetSet = mutable.Set(target:_*)

    source.distance = 0
    targetSet.remove(source)

    queue.clear()
    queue.enqueue(source)

    while (!queue.empty && targetSet.nonEmpty) {
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
        targetSet.remove(curr)
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
