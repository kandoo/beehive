package openflow

import (
	"errors"
	"flag"
	"fmt"
	"net"

	"github.com/golang/glog"
	"github.com/soheilhy/beehive/bh"
	"github.com/soheilhy/beehive/openflow/of"
	"github.com/soheilhy/beehive/openflow/of10"
)

// OFConfig stores the configuration of the OpenFlow driver.
type OFConfig struct {
	Proto string // The driver's listening protocol.
	Addr  string // The driver's listening address.
}

var defaultOFConfig = OFConfig{}

func init() {
	flag.StringVar(&defaultOFConfig.Proto, "ofproto", "tcp",
		"Protocol of the OpenFlow listener.")
	flag.StringVar(&defaultOFConfig.Addr, "ofaddr", "0.0.0.0:6633",
		"Address of the OpenFlow listener in the form of HOST:PORT.")
}

// StartOpenFlow starts the OpenFlow driver on the given hive using the default
// OpenFlow configuration that can be set through command line arguments.
func StartOpenFlow(hive bh.Hive) error {
	return StartOpenFlowWithConfig(hive, defaultOFConfig)
}

// StartOpenFlowWithConfig starts the OpenFlow driver on the give hive with the
// provided configuration.
func StartOpenFlowWithConfig(hive bh.Hive, cfg OFConfig) error {
	app := hive.NewApp("OFDriver")
	err := app.Detached(&ofListener{
		cfg: cfg,
	})
	if err != nil {
		return err
	}

	app.Handle(ofMsg{}, &ofDriver{})
	return nil
}

type ofDriver struct{}

type ofListener struct {
	cfg OFConfig
}

type ofConn struct {
	of.HeaderConn
	listener *ofListener
	ctx      bh.RcvContext
	outCh    chan of.Header
	closeCh  chan bool
	swtch    of10.FeaturesReply
}

type ofMsg struct {
	pkt  of10.Header10
	conn *ofConn
}

func (d *ofDriver) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	of := msg.Data().(ofMsg)
	switch {
	case of10.IsFeaturesReply(of.pkt):
		r, _ := of10.ToFeaturesReply(of.pkt)
		glog.Infof("Switch joined %016x", r.DatapathId())
		for _, p := range r.Ports() {
			glog.Infof("Port (switch=%016x, no=%d, mac=%012x, name=%s)\n",
				r.DatapathId(), p.PortNo(), p.HwAddr(), p.Name())
		}

		ctx.SetBeeLocal(of.conn)

		glog.Infof("Disabling packet buffers in the switch.")
		c := of10.NewSwitchSetConfig()
		c.SetMissSendLen(0xFFFF)
		of.conn.outCh <- c.Header

	case of10.IsPacketIn(of.pkt):
		in, _ := of10.ToPacketIn(of.pkt)
		out := of10.NewPacketOut()
		out.SetBufferId(in.BufferId())
		out.SetInPort(in.InPort())

		bcast := of10.NewActionOutput()
		bcast.SetPort(uint16(of10.PP_FLOOD))

		out.AddActions(bcast.ActionHeader)
		for _, d := range in.Data() {
			out.AddData(d)
		}
		of.conn.outCh <- out.Header
	}
	return nil
}

func (d *ofDriver) Map(msg bh.Msg, ctx bh.MapContext) bh.MapSet {
	c := msg.Data().(ofMsg).conn
	k := fmt.Sprintf("%s/%x", c.RemoteAddr().String(), c.swtch.DatapathId())
	return bh.MapSet{{"D", bh.Key(k)}}
}

func (l *ofListener) Start(ctx bh.RcvContext) {
	nl, err := net.Listen(l.cfg.Proto, l.cfg.Addr)
	if err != nil {
		glog.Errorf("Cannot start the OF listener: %v", err)
		return
	}

	glog.Infof("OF listener started")

	defer func() {
		glog.Infof("OF listener closed")
		nl.Close()
	}()

	for {
		c, err := nl.Accept()
		if err != nil {
			glog.Errorf("Error in OF accept: %v", err)
			return
		}

		go l.startOFConn(c, ctx)
	}
}

func (l *ofListener) startOFConn(conn net.Conn, ctx bh.RcvContext) {
	ofc := &ofConn{
		HeaderConn: of.NewHeaderConn(conn),
		listener:   l,
		closeCh:    make(chan bool),
		outCh:      make(chan of.Header),
		ctx:        ctx,
	}

	if err := ofc.handshake(); err != nil {
		glog.Errorf("Error in OpenFlow handshake: %v", err)
		ofc.Close()
		return
	}

	go ofc.reader()
	go ofc.writer()
}

func (c *ofConn) handshake() error {
	pkts := make([]of.Header, 1)
	_, err := c.Read(pkts)
	if err != nil {
		return err
	}

	h, err := of.ToHello(pkts[0])
	if err != nil {
		return err
	}

	glog.V(2).Info("Received hello from a switch")

	h.SetVersion(uint8(of.OPENFLOW_1_0))
	if err = c.Write([]of.Header{h.Header}); err != nil {
		return err
	}

	glog.V(2).Info("Sent hello to the switch")

	freq := of10.NewFeaturesRequest()
	if err = c.Write([]of.Header{freq.Header}); err != nil {
		return err
	}

	glog.V(2).Info("Sent features request to the switch")

	_, err = c.Read(pkts)
	if err != nil {
		return err
	}

	v10, err := of10.ToHeader10(pkts[0])
	if err != nil {
		return err
	}

	frep, err := of10.ToFeaturesReply(v10)
	if err != nil {
		return err
	}

	frep, err = frep.Clone()
	if err != nil {
		return err
	}

	glog.Infof("Handshake completed for switch %016x", frep.DatapathId())

	c.swtch = frep
	c.ctx.Emit(ofMsg{
		pkt:  frep.Header10,
		conn: c,
	})
	return nil
}

func (c *ofConn) reader() {
	pkts := make([]of.Header, 10)
	defer func() {
		c.Close()
	}()

	for {
		n, err := c.Read(pkts)
		if err != nil {
			glog.Errorf("Cannot read from the connection: %v", err)
			return
		}

		for _, pkt := range pkts[:n] {
			pkt10, err := of10.ToHeader10(pkt)
			if err != nil {
				glog.Errorf("OF Driver only support OF v1.0")
				return
			}

			switch {
			case of10.IsEchoRequest(pkt10):
				glog.V(2).Infof("Received an echo request from the switch")
				rep := of10.NewEchoReply()
				rep.SetXid(pkt.Xid())
				err := c.Write([]of.Header{rep.Header})
				if err != nil {
					glog.Errorf("Error in writing an echo reply: %v", err)
					return
				}
				glog.V(2).Infof("Sent an echo reply from the switch")
			default:
				c.ctx.Emit(ofMsg{pkt10, c})
			}
		}

		pkts = pkts[n:]
		if len(pkts) == 0 {
			pkts = make([]of.Header, 10)
		}
	}
}

func (c *ofConn) writer() {
	defer c.Close()
	for {
		select {
		case pkt := <-c.outCh:
			c.Write([]of.Header{pkt})
		case <-c.closeCh:
			return
		}
	}
}

func (l *ofListener) Stop(ctx bh.RcvContext) {
}

func (l *ofListener) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	return errors.New("No message should be sent to the listener")
}
