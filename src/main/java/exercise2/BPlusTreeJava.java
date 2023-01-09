package exercise2;

import com.sun.jdi.Value;
import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.exercise2.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Stack;

/**
 * This is the B+-Tree implementation you will work on.
 * Your task is to implement the insert-operation.
 *
 */
@ChosenImplementation(true)
public class BPlusTreeJava extends AbstractBPlusTree {
    public BPlusTreeJava(int order) {
        super(order);
    }

    public BPlusTreeJava(BPlusTreeNode<?> rootNode) {
        super(rootNode);
    }

    @Nullable
    @Override
    public ValueReference insert(@NotNull Integer key, @NotNull ValueReference value) {
        // Find LeafNode and the path to the LeafNode in which the key has to be inserted.
        LeafNode leafNode;
        Stack<BPlusTreeNode> path = findPathToInsertLeaf(key);
        leafNode = (LeafNode) path.pop();


        if(!leafNode.isFull()) {
            ValueReference result = insertIntoLeaf(leafNode, key, value);
            return result;
        }
        else {
            // Otherwise
            //   Split the LeafNode in two!
            //   Is parent node root?
            //     update rootNode = ... // will have only one key
            //   Was node instanceof LeafNode?
            //     update parentNode.keys[?] = ...
            //   Don't forget to update the parent keys and so on...
            insertWithSplit(leafNode, key, value, path);

        }
        return null;
    }


    public Stack<BPlusTreeNode> findPathToInsertLeaf(int key){

        BPlusTreeNode root = this.getRootNode();
        Stack<BPlusTreeNode> path = new Stack<>();
        if(root instanceof InitialRootNode){
            path.push(root);
            return path;
        }




        BPlusTreeNode current = root;
        path.push(current);
        for (int i = root.getHeight(); i > 1; i--) {
            current = ((InnerNode) current).selectChild(key);
            path.push(current);
        }
        path.push((LeafNode)((InnerNode)current).selectChild(key));

        return path;
    }

    public ValueReference insertIntoLeaf(LeafNode leafNode, Integer key, ValueReference value){
        for(int i = 0; i < leafNode.keys.length; i++) {
            // key already exits -> overwrite and return old value
            if (key.equals(leafNode.keys[i])) {

                ValueReference oldRef = (ValueReference) leafNode.references[i];
                leafNode.references[i] = value;
                return oldRef;
            }
            else if ( leafNode.keys[i] == null || key < leafNode.keys[i]) {
                // found the position to insert the key
                // now move every key one index further in the array
                for (int j = leafNode.keys.length - 2; j >= i; j--) {
                    leafNode.keys[j + 1] = leafNode.keys[j];
                    leafNode.references[j + 1] = leafNode.references[j];
                }
                leafNode.keys[i] = key;
                leafNode.references[i] = value;
                return null;
            }
        }
        return null;
    }

    public void insertWithSplit(LeafNode leafNode, Integer key, ValueReference value, Stack<BPlusTreeNode> path){
        Entry newLeafEntries[] = new Entry[(int)Math.ceil(order/2)];
        int leftIndex = order-2;
        Boolean keyUsed = false;
        for (int i = (int) Math.ceil(order/2)-1; i >=0; i--) {
            if(key> leafNode.keys[leftIndex] && !keyUsed){
                newLeafEntries[i] = new Entry(key, value);
                keyUsed = true;
            }else{
                newLeafEntries[i] = new Entry(leafNode.keys[leftIndex],leafNode.references[leftIndex]);
                leafNode.keys[leftIndex] = null;
                leafNode.references[leftIndex] = null;
                leftIndex--;
            }
        }
        if(!keyUsed){
            while(leftIndex>=0){
                if(key<leafNode.keys[leftIndex]){
                    leafNode.keys[leftIndex+1] = leafNode.keys[leftIndex];
                    leafNode.references[leftIndex+1] = leafNode.references[leftIndex];
                    if(leftIndex == 0){
                        leafNode.keys[leftIndex] = key;
                        leafNode.references[leftIndex] = value;
                    }
                    leftIndex--;

                }else{
                    leafNode.keys[leftIndex+1] = key;
                    leafNode.references[leftIndex+1] = value;
                    break;
                }
            }
        }

        LeafNode newLeaf = new LeafNode(order, newLeafEntries);
        newLeaf.nextSibling = leafNode.nextSibling;
        leafNode.nextSibling = newLeaf;
        if(leafNode == getRootNode()){
            Entry newRootEntries[] = new Entry[(int)Math.floor(order/2)];
            for(int i = 0; i<newRootEntries.length; i++){
                newRootEntries[i] = new Entry(leafNode.keys[i],leafNode.references[i]);
            }
            LeafNode leftLeaf = new LeafNode(order, newRootEntries);
            leftLeaf.nextSibling = newLeaf;
            
            rootNode = new InnerNode(order, leftLeaf ,newLeaf);
            return;
        }


        Integer keyToInsert = newLeaf.getSmallestKey();
        BPlusTreeNode refToInsert = newLeaf;
        while(!path.isEmpty()){
            if(keyToInsert == null){
                break;
            }else{
                InnerNode parentNode = (InnerNode) path.pop();
                if(!parentNode.isFull()){
                    for(int i = 0; i < parentNode.keys.length;i++){
                        if(parentNode.keys[i] == null || keyToInsert < parentNode.keys[i] ){

                            for(int j = parentNode.keys.length-2; j>=i; j--){
                                parentNode.keys[j+1] = parentNode.keys[j];
                                parentNode.references[j+2] = parentNode.references[j+1];
                            }
                            parentNode.keys[i] = keyToInsert;
                            parentNode.references[i+1] = refToInsert;

                            keyToInsert = null;
                            break;
                        }
                    }
                }else{
                    BPlusTreeNode newNodeRefs[] = new BPlusTreeNode[(int)Math.floor((order+1)/2)];
                    leftIndex = order-2;
                    keyUsed = false;
                    for (int rightIndex = (int) Math.floor((order+1)/2)-1; rightIndex >=0; rightIndex--) {
                        if(keyToInsert > parentNode.keys[leftIndex] && !keyUsed){
                            newNodeRefs[rightIndex] = refToInsert;
                            keyUsed = true;
                        }else{
                            newNodeRefs[rightIndex] = parentNode.references[leftIndex+1];

                            parentNode.keys[leftIndex] = null;
                            parentNode.references[leftIndex+1] = null;
                            leftIndex--;
                        }
                    }
                    //leftIndex--;
                    if(!keyUsed){
                        while(leftIndex>=0){
                            if(keyToInsert < parentNode.keys[leftIndex]){
                                parentNode.keys[leftIndex+1] = parentNode.keys[leftIndex];
                                parentNode.references[leftIndex+2] = parentNode.references[leftIndex+1];
                                if(leftIndex == 0){
                                    parentNode.keys[leftIndex] = keyToInsert;
                                    parentNode.references[leftIndex+1] = refToInsert;
                                }
                                leftIndex--;
                            }else{
                                parentNode.keys[leftIndex] = keyToInsert;
                                parentNode.references[leftIndex+1] = refToInsert;
                            }
                        }
                    }
                    InnerNode newNode = new InnerNode(order,newNodeRefs);
                    keyToInsert = newNode.getSmallestKey();
                    refToInsert = newNode;

                    if(parentNode == getRootNode()){
                        rootNode = new InnerNode(order,parentNode,refToInsert);
                    }

                }

            }


        }

    }



    // Check out the exercise slides for a flow chart of this logic.
    // If you feel stuck, try to draw what you want to do and
    // check out Ex2Main for playing around with the tree by e.g. printing or debugging it.
    // Also check out all the methods on BPlusTreeNode and how they are implemented or
    // the tests in BPlusTreeNodeTests and BPlusTreeTests!
}