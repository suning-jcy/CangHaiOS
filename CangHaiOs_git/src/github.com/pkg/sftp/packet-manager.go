package sftp

import (
	"encoding"
	//"time"
	//"hash/crc32"
	"sync"

	"code.google.com/p/weed-fs/go/glog"
)

// The goal of the packetManager is to keep the outgoing packets in the same
// order as the incoming. This is due to some sftp clients requiring this
// behavior (eg. winscp).

type packetSender interface {
	sendPacket(encoding.BinaryMarshaler) error
}

type packetManager struct {
	requests  chan requestPacket
	responses chan responsePacket
	lastoutid uint32
	fini      chan struct{}
	incoming  requestPacketIDs
	outgoing  responsePackets
	sender    packetSender // connection object
	working   *sync.WaitGroup
	workernum int
}

func newPktMgr(sender packetSender, worknum int) packetManager {
	//sftpServerWorkerCount = worknum
	s := packetManager{
		requests:  make(chan requestPacket, worknum),
		responses: make(chan responsePacket, worknum),
		fini:      make(chan struct{}),
		incoming:  make([]uint64, 0, worknum),
		outgoing:  make([]responsePacket, 0, worknum),
		sender:    sender,
		working:   &sync.WaitGroup{},
		workernum: worknum,
	}
	go s.controller()
	return s
}

// register incoming packets to be handled
// send id of 0 for packets without id
func (s packetManager) incomingPacket(pkt requestPacket) {
	s.working.Add(1)
	glog.V(3).Infoln("wait to send packet", pkt.uid())
	s.requests <- pkt // buffer == sftpServerWorkerCount
}

// register outgoing packets as being ready
func (s packetManager) readyPacket(pkt responsePacket) {
	glog.V(3).Infoln("want to send packet", pkt.uid())
	s.responses <- pkt
	s.working.Done()
}

// shut down packetManager controller
func (s packetManager) close() {
	// pause until current packets are processed
	s.working.Wait()
	close(s.fini)
}

// Passed a worker function, returns a channel for incoming packets.
// The goal is to process packets in the order they are received as is
// requires by section 7 of the RFC, while maximizing throughput of file
// transfers.
func (s *packetManager) workerChan(runWorker func(requestChan)) requestChan {

	/*
		rwChans := make([]chan requestPacket, sftpServerWorkerCount)
		for i := 0; i < sftpServerWorkerCount; i++ {
			rwChan := make(chan requestPacket, sftpServerWorkerCount)
			rwChans[i] = rwChan
			runWorker(rwChan)
		}
	*/
	rwChan := make(chan requestPacket, s.workernum)
	for i := 0; i < s.workernum; i++ {
		runWorker(rwChan)
	}
	cmdChan := make(chan requestPacket)
	runWorker(cmdChan)

	pktChan := make(chan requestPacket, s.workernum)
	go func() {
		// start with cmdChan
		curChan := cmdChan
		for pkt := range pktChan {
			ttt := ""
			// on file open packet, switch to rwChan
			switch pkt.(type) {
			case *sshFxpOpenPacket:
				ttt = "is sshFxpOpenPacket"

				curChan = rwChan
			// on file close packet, switch back to cmdChan
			// after waiting for any reads/writes to finish
			case *sshFxpClosePacket:
				ttt = "is sshFxpClosePacket"

				// wait for rwChan to finish
				s.working.Wait()
				// stop using rwChan
				curChan = cmdChan
			}
			glog.V(4).Infoln("get packet", pkt.uid(), ttt)
			s.incomingPacket(pkt)
			curChan <- pkt
		}
		//for i := 0; i < sftpServerWorkerCount; i++ {
		//	close(rwChans[i])
		//}
		close(rwChan)
		close(cmdChan)
		s.close()
	}()

	return pktChan
}

// process packets
func (s *packetManager) controller() {
	for {
		select {
		case pkt := <-s.requests:
			debug("incoming id: %v", pkt.uid())
			s.incoming = append(s.incoming, pkt.uid())
			if len(s.incoming) > 1 {
				s.incoming.Sort()
			}
		case pkt := <-s.responses:
			debug("outgoing pkt: %v", pkt.uid())
			s.outgoing = append(s.outgoing, pkt)
			if len(s.outgoing) > 1 {
				s.outgoing.Sort()
			}
		case <-s.fini:
			return
		}
		s.maybeSendPackets()
	}
}

// send as many packets as are ready
func (s *packetManager) maybeSendPackets() {
	for {
		if len(s.outgoing) == 0 || len(s.incoming) == 0 {
			debug("break! -- outgoing: %v; incoming: %v",
				len(s.outgoing), len(s.incoming))
			break
		}
		out := s.outgoing[0]
		in := s.incoming[0]
		// 		debug("incoming: %v", s.incoming)
		// 		debug("outgoing: %v", outfilter(s.outgoing))
		if in == out.uid() {
			glog.V(3).Infoln("send packet", out.uid(), "start")
			if testt, ok := out.(*sshFxpDataPacket); ok {
				glog.V(3).Infoln("responsePacket", testt.ID)
			}

			s.sender.sendPacket(out)
			glog.V(3).Infoln("send packet", out.uid(), "OK")
			// pop off heads
			copy(s.incoming, s.incoming[1:])            // shift left
			s.incoming = s.incoming[:len(s.incoming)-1] // remove last
			copy(s.outgoing, s.outgoing[1:])            // shift left
			s.outgoing = s.outgoing[:len(s.outgoing)-1] // remove last
		} else {
			glog.V(3).Infoln("incoming:", in, "outgoing:", out.uid())
			//time.Sleep(time.Duration(10) * time.Millisecond)
			break
		}
	}
}

func outfilter(o []responsePacket) []uint32 {
	res := make([]uint32, 0, len(o))
	for _, v := range o {
		res = append(res, v.id())
	}
	return res
}
