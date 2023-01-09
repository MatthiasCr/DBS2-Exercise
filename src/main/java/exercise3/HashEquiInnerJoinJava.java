package exercise3;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.exercise3.InnerJoinOperation;
import de.hpi.dbs2.exercise3.JoinAttributePair;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

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
		throw new UnsupportedOperationException();
	}

	@Override
	public void join(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation,
		@NotNull Relation outputRelation
	) {
		BlockManager blockmanager = getBlockManager();

		// calculate a sensible bucket count
		int bucketCount = getBlockManager().getFreeBlocks() - 1;

		//  - hash relation
		ArrayList<LinkedList<Block>> bucketsLeftRelation = initializeBucketList(bucketCount);
		ArrayList<LinkedList<Block>> bucketsRightRelation = initializeBucketList(bucketCount);

		hashRelation(leftInputRelation, bucketsLeftRelation, getJoinAttributePair().getLeftColumnIndex(), bucketCount);
		hashRelation(rightInputRelation, bucketsRightRelation, getJoinAttributePair().getRightColumnIndex(), bucketCount);

		//  - join hashed blocks


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

	public void hashRelation(
			Relation relation,
			ArrayList<LinkedList<Block>> bucketList,
			int hashAttrColumnIndex, int bucketCount
	) {
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

				LinkedList<Block> bucket = bucketList.get(hash);
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
		}
	}
}
