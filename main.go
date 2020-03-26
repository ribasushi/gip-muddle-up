package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	blkfmt "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	gocar "github.com/ipld/go-car"
	_ "github.com/ipld/go-ipld-prime/encoding/dagcbor"
	gipfree "github.com/ipld/go-ipld-prime/impl/free"
	gipselector "github.com/ipld/go-ipld-prime/traversal/selector"
	gipselectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var sourceFn string = "./lotus_testnet_export_128_shuffled_nulroot.car"

func main() {

	inFile, err := os.Open(sourceFn)
	if err != nil {
		log.Fatalf("Unable to open source '%s': %s", sourceFn, err)
	}

	carIn, _ := gocar.NewCarReader(inFile)

	blockStore := make(map[cid.Cid]blkfmt.Block)
	for {
		b, _ := carIn.Next()
		if b == nil {
			break
		}

		blockStore[b.Cid()] = b
	}

	root, _ := cid.Decode("bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy")

	var wrapper getFromBlockStore = func(c cid.Cid) (blkfmt.Block, error) {
		//log.Printf("Blockstore get %s\n", c.String())
		if blk, exists := blockStore[c]; exists {
			return blk, nil
		}
		return nil, fmt.Errorf("Cid %s not found...", c)
	}

	sb := gipselectorbuilder.NewSelectorSpecBuilder(gipfree.NodeBuilder())
	carOut := gocar.NewSelectiveCar(
		context.Background(),
		&wrapper,
		[]gocar.Dag{gocar.Dag{
			Root: root,
			Selector: sb.ExploreRecursive(
				gipselector.RecursionLimitNone(),
				sb.ExploreAll(sb.ExploreRecursiveEdge()),
			).Node(),
		}},
	)

	pipeR, pipeW := io.Pipe()
	errCh := make(chan error, 2) // we only report the 1st error
	go func() {
		defer func() {
			if err := pipeW.Close(); err != nil {
				errCh <- fmt.Errorf("stream flush failed: %s", err)
			}
			close(errCh)
		}()

		if err := carOut.Write(pipeW); err != nil {
			errCh <- err
		}
	}()

	go func() {
		buf := make([]byte, 1024*1024)
		for {
			n, err := pipeR.Read(buf)
			log.Printf("Written %d bytes of .car\n", n)

			if err != nil {
				return
			}
		}
	}()

	if err := <-errCh; err != nil {
		log.Fatalf("Car write failed: %s", err)
	}

	pipeW.Close() // ignore errors if any
}

type getFromBlockStore func(cid.Cid) (blkfmt.Block, error)

func (w *getFromBlockStore) Get(c cid.Cid) (blkfmt.Block, error) {
	return (*w)(c)
}
