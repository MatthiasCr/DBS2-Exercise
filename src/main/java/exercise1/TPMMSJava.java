package exercise1;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.BlockManager;
import de.hpi.dbs2.dbms.Relation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.dbms.utils.BlockSorter;
import de.hpi.dbs2.exercise1.SortOperation;
import org.checkerframework.checker.units.qual.A;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@ChosenImplementation(true)
public class TPMMSJava extends SortOperation {
    public TPMMSJava(
        @NotNull BlockManager manager,
        int sortColumnIndex
    ) {
        super(manager, sortColumnIndex);
    }

    @Override
    public int estimatedIOCost(@NotNull Relation relation) {
        BlockManager blockManager = getBlockManager();
        int totalBlocks = blockManager.getFreeBlocks();

        if ((totalBlocks * (totalBlocks - 1)) < relation.getEstimatedSize()) {
            throw new RelationSizeExceedsCapacityException();
        }

        int ioCost = 0;

        // Phase 1
        ioCost += 2 * relation.getEstimatedSize();
        // Phase 2
        ioCost += 2 * relation.getEstimatedSize();
        return ioCost;
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput output) {
        BlockManager blockManager = getBlockManager();

        int totalBlocks = blockManager.getFreeBlocks();
        System.out.println("Total Blocks: " + totalBlocks);
        System.out.println("Relation Estimated Size: " + relation.getEstimatedSize());

        if ((totalBlocks * (totalBlocks - 1)) < relation.getEstimatedSize()) {
            throw new RelationSizeExceedsCapacityException();
        }
        Comparator<Tuple> columnComp = relation.getColumns().getColumnComparator(getSortColumnIndex());

        // -------
        // Phase 1
        // -------
        ArrayList<ArrayList<Block>> sublists = new ArrayList<ArrayList<Block>>();

        Iterator<Block> iter = relation.iterator();
        while(iter.hasNext()) {

            int i = totalBlocks;
            ArrayList<Block> sublist = new ArrayList<Block>();

            while (i > 0 && iter.hasNext()) {
                Block currentBlock = iter.next();
                blockManager.load(currentBlock);
                sublist.add(currentBlock);
                i--;
            }
            BlockSorter.INSTANCE.sort(relation, sublist, columnComp);
            for (int j = sublist.size() - 1; j >= 0; j--){
                blockManager.release(sublist.get(j),true);
            }

            sublists.add(sublist);
        }

        // -------
        // Phase 2
        // -------
        Block outputBlock = blockManager.allocate(true);

        // load first block of every sublist into memory
        Block[] currentMemoryBlocks = new Block[sublists.size()];
        for (int i = 0; i < sublists.size(); i++) {
            Block block = sublists.get(i).get(0);
            currentMemoryBlocks[i] = block;
            blockManager.load(block);

            sublists.get(i).remove(0);
        }

        int[] blockPositions = new int[currentMemoryBlocks.length];

        for (int i = 0; i < relation.getEstimatedSize(); i++) {
        // We will obviously need as many output blocks as the original relation does

            // Fill output block
            while (!outputBlock.isFull()) {

                // search for minimum tuple
                Tuple minTuple = null;
                int minTupleBlock = 0;

                // iterate over all blocks to find minimum tuple
                for (int j = 0; j < currentMemoryBlocks.length; j++) {

                    if (currentMemoryBlocks[j] == null) {
                        // skip if every block from sublist j is already covered
                        continue;
                    }

                    Tuple currentTuple = currentMemoryBlocks[j].get(blockPositions[j]);
                    if (minTuple == null) {
                        minTuple = currentTuple;
                        minTupleBlock = j;
                    }
                    if (columnComp.compare(minTuple, currentTuple) > 0) {
                        minTuple = currentTuple;
                        minTupleBlock = j;
                    }
                }
                outputBlock.append(minTuple);
                blockPositions[minTupleBlock]++;

                // check for empty block and reload
                if (blockPositions[minTupleBlock] >= currentMemoryBlocks[minTupleBlock].getSize()) {
                    // block is empty
                    blockManager.release(currentMemoryBlocks[minTupleBlock], false);

                    if (!sublists.get(minTupleBlock).isEmpty()) {
                        // next block can be loaded
                        blockManager.load(sublists.get(minTupleBlock).get(0));
                        currentMemoryBlocks[minTupleBlock] = sublists.get(minTupleBlock).get(0);
                        sublists.get(minTupleBlock).remove(0);

                        blockPositions[minTupleBlock] = 0;
                    }
                    else {
                        currentMemoryBlocks[minTupleBlock] = null;
                    }
                }
                Boolean allBlocksEmpty = true;
                for (int j = 0; j < currentMemoryBlocks.length; j++) {
                    if (!(currentMemoryBlocks == null)){
                        allBlocksEmpty = false;
                    }
                }
                if (allBlocksEmpty) {
                    break;
                }
            }
            // output the output block
            output.output(outputBlock);
        }
        blockManager.release(outputBlock, false);
    }
}