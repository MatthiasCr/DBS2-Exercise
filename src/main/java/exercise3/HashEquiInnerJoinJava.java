package exercise3;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.exercise3.InnerJoinOperation;
import de.hpi.dbs2.exercise3.JoinAttributePair;
import de.hpi.dbs2.exercise3.NestedLoopEquiInnerJoin;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Consumer;

@ChosenImplementation(true)
public class HashEquiInnerJoinJava extends InnerJoinOperation {

	public HashEquiInnerJoinJava(
		@NotNull BlockManager blockManager, int leftColumnIndex, int rightColumnIndex
	) {
		super(blockManager, new JoinAttributePair.EquiJoinAttributePair(leftColumnIndex, rightColumnIndex));
	}

	@Override
	public int estimatedIOCost(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation
	) {
		int bucketCount = getBlockManager().getFreeBlocks() - 1;
		int rightRelationBucketSize = rightInputRelation.estimatedBlockCount() / bucketCount;
		int leftRelationBucketSize = leftInputRelation.estimatedBlockCount() / bucketCount;

		if (Math.min(rightRelationBucketSize, leftRelationBucketSize) > getBlockManager().getFreeBlocks() - 2) {
			throw new RelationSizeExceedsCapacityException();
		}
		return 3 * (leftInputRelation.estimatedBlockCount() + rightInputRelation.estimatedBlockCount());
	}

	@Override
	public void join(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation,
		@NotNull Relation outputRelation
	) {
		BlockManager blockmanager = getBlockManager();

		// calculate a sensible bucket count
		int bucketCount = getBlockManager().getFreeBlocks() - 1;

		// hash relation
		ArrayList<LinkedList<Block>> bucketsLeftRelation
				= hashRelation(leftInputRelation, getJoinAttributePair().getLeftColumnIndex(), bucketCount);
		ArrayList<LinkedList<Block>> bucketsRightRelation
				= hashRelation(rightInputRelation, getJoinAttributePair().getRightColumnIndex(), bucketCount);


		// join hashed blocks
		int rightRelationBucketSize = rightInputRelation.estimatedBlockCount() / bucketCount;
		int leftRelationBucketSize = leftInputRelation.estimatedBlockCount() / bucketCount;

		if (Math.min(rightRelationBucketSize, leftRelationBucketSize) > getBlockManager().getFreeBlocks() - 2) {
			throw new RelationSizeExceedsCapacityException();
		}

		boolean swapped = rightRelationBucketSize < leftRelationBucketSize;
		ArrayList<LinkedList<Block>> outerBuckets = (swapped) ? bucketsRightRelation : bucketsLeftRelation;
		ArrayList<LinkedList<Block>> innerBuckets = (swapped) ? bucketsLeftRelation : bucketsRightRelation;

		TupleAppender tupleAppender = new TupleAppender(outputRelation.getBlockOutput());
		for(int i = 0; i < bucketCount; i++) {
			LinkedList<Block> outerBucket = outerBuckets.get(i);
			LinkedList<Block> innerBucket = innerBuckets.get(i);

			for (Block block : outerBucket) {
				getBlockManager().load(block);
			}
			for(Block innerBlockRef : innerBucket) {
				Block innerBlock = getBlockManager().load(innerBlockRef);
				for (Block outerBlock : outerBucket) {
					joinBlocks(
							swapped ? innerBlock : outerBlock,
							swapped ? outerBlock : innerBlock,
							outputRelation.getColumns(),
							tupleAppender
					);
				}
				getBlockManager().release(innerBlock, false);
			}
			for (Block block : outerBucket) {
				getBlockManager().release(block, true);
			}
		}
		tupleAppender.close();
	}

	public ArrayList<LinkedList<Block>> hashRelation(
			Relation relation,
			int hashAttrColumnIndex, int bucketCount
	) {
		ArrayList<LinkedList<Block>> buckets = initializeBucketList(bucketCount);

		Iterator<Block> relationIter = relation.iterator();

		Block currentBlock;
		while (relationIter.hasNext()) {
			currentBlock = relationIter.next();
			getBlockManager().load(currentBlock);
			Iterator<Tuple> currentBlockIter = currentBlock.iterator();

			Tuple currentTuple;
			while (currentBlockIter.hasNext()) {
				currentTuple = currentBlockIter.next();
				int hash = currentTuple.get(hashAttrColumnIndex).hashCode() % bucketCount;
				if (hash < 0) {
					hash += bucketCount;
				}

				LinkedList<Block> bucket = buckets.get(hash);
				Block block = bucket.getLast();

				if (block.isFull()) {
					getBlockManager().release(block, true);
					Block newBlock = getBlockManager().allocate(true);
					newBlock.append(currentTuple);
					bucket.addLast(newBlock);
				} else {
					block.append(currentTuple);
				}
			}
			getBlockManager().release(currentBlock, false);
		}
		for (int i = 0; i < bucketCount; i++) {
			Block buffer = buckets.get(i).getLast();
			if (buffer.isLoaded() && !buffer.isEmpty()) {
				getBlockManager().release(buffer, true);
			}
		}

		return buckets;
	}

	public ArrayList<LinkedList<Block>> initializeBucketList(int bucketCount) {
		ArrayList<LinkedList<Block>> buckets = new ArrayList<>(bucketCount);
		for (int i = 0; i < bucketCount; i++) {
			LinkedList<Block> bucket = new LinkedList<>();
			Block block = getBlockManager().allocate(true);
			bucket.addLast(block);

			buckets.add(i, bucket);
		}
		return buckets;
	}


	class TupleAppender implements AutoCloseable, Consumer<Tuple> {

		BlockOutput blockOutput;

		TupleAppender(BlockOutput blockOutput) {
			this.blockOutput = blockOutput;
		}

		Block outputBlock = getBlockManager().allocate(true);

		@Override
		public void accept(Tuple tuple) {
			if(outputBlock.isFull()) {
				blockOutput.move(outputBlock);
				outputBlock = getBlockManager().allocate(true);
			}
			outputBlock.append(tuple);
		}

		@Override
		public void close() {
			if(!outputBlock.isEmpty()) {
				blockOutput.move(outputBlock);
			} else {
				getBlockManager().release(outputBlock, false);
			}
		}
	}
}
