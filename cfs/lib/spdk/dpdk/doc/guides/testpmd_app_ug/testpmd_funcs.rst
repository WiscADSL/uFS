..  SPDX-License-Identifier: BSD-3-Clause
    Copyright(c) 2010-2016 Intel Corporation.

.. _testpmd_runtime:

Testpmd Runtime Functions
=========================

Where the testpmd application is started in interactive mode, (``-i|--interactive``),
it displays a prompt that can be used to start and stop forwarding,
configure the application, display statistics (including the extended NIC
statistics aka xstats) , set the Flow Director and other tasks::

   testpmd>

The testpmd prompt has some, limited, readline support.
Common bash command-line functions such as ``Ctrl+a`` and ``Ctrl+e`` to go to the start and end of the prompt line are supported
as well as access to the command history via the up-arrow.

There is also support for tab completion.
If you type a partial command and hit ``<TAB>`` you get a list of the available completions:

.. code-block:: console

   testpmd> show port <TAB>

       info [Mul-choice STRING]: show|clear port info|stats|xstats|fdir|stat_qmap|dcb_tc|cap X
       info [Mul-choice STRING]: show|clear port info|stats|xstats|fdir|stat_qmap|dcb_tc|cap all
       stats [Mul-choice STRING]: show|clear port info|stats|xstats|fdir|stat_qmap|dcb_tc|cap X
       stats [Mul-choice STRING]: show|clear port info|stats|xstats|fdir|stat_qmap|dcb_tc|cap all
       ...


.. note::

   Some examples in this document are too long to fit on one line are are shown wrapped at `"\\"` for display purposes::

      testpmd> set flow_ctrl rx (on|off) tx (on|off) (high_water) (low_water) \
               (pause_time) (send_xon) (port_id)

In the real ``testpmd>`` prompt these commands should be on a single line.

Help Functions
--------------

The testpmd has on-line help for the functions that are available at runtime.
These are divided into sections and can be accessed using help, help section or help all:

.. code-block:: console

   testpmd> help

       help control    : Start and stop forwarding.
       help display    : Displaying port, stats and config information.
       help config     : Configuration information.
       help ports      : Configuring ports.
       help registers  : Reading and setting port registers.
       help filters    : Filters configuration help.
       help all        : All of the above sections.


Command File Functions
----------------------

To facilitate loading large number of commands or to avoid cutting and pasting where not
practical or possible testpmd supports alternative methods for executing commands.

* If started with the ``--cmdline-file=FILENAME`` command line argument testpmd
  will execute all CLI commands contained within the file immediately before
  starting packet forwarding or entering interactive mode.

.. code-block:: console

   ./testpmd -n4 -r2 ... -- -i --cmdline-file=/home/ubuntu/flow-create-commands.txt
   Interactive-mode selected
   CLI commands to be read from /home/ubuntu/flow-create-commands.txt
   Configuring Port 0 (socket 0)
   Port 0: 7C:FE:90:CB:74:CE
   Configuring Port 1 (socket 0)
   Port 1: 7C:FE:90:CB:74:CA
   Checking link statuses...
   Port 0 Link Up - speed 10000 Mbps - full-duplex
   Port 1 Link Up - speed 10000 Mbps - full-duplex
   Done
   Flow rule #0 created
   Flow rule #1 created
   ...
   ...
   Flow rule #498 created
   Flow rule #499 created
   Read all CLI commands from /home/ubuntu/flow-create-commands.txt
   testpmd>


* At run-time additional commands can be loaded in bulk by invoking the ``load FILENAME``
  command.

.. code-block:: console

   testpmd> load /home/ubuntu/flow-create-commands.txt
   Flow rule #0 created
   Flow rule #1 created
   ...
   ...
   Flow rule #498 created
   Flow rule #499 created
   Read all CLI commands from /home/ubuntu/flow-create-commands.txt
   testpmd>


In all cases output from any included command will be displayed as standard output.
Execution will continue until the end of the file is reached regardless of
whether any errors occur.  The end user must examine the output to determine if
any failures occurred.


Control Functions
-----------------

start
~~~~~

Start packet forwarding with current configuration::

   testpmd> start

start tx_first
~~~~~~~~~~~~~~

Start packet forwarding with current configuration after sending specified number of bursts of packets::

   testpmd> start tx_first (""|burst_num)

The default burst number is 1 when ``burst_num`` not presented.

stop
~~~~

Stop packet forwarding, and display accumulated statistics::

   testpmd> stop

quit
~~~~

Quit to prompt::

   testpmd> quit


Display Functions
-----------------

The functions in the following sections are used to display information about the
testpmd configuration or the NIC status.

show port
~~~~~~~~~

Display information for a given port or all ports::

   testpmd> show port (info|stats|xstats|fdir|stat_qmap|dcb_tc|cap) (port_id|all)

The available information categories are:

* ``info``: General port information such as MAC address.

* ``stats``: RX/TX statistics.

* ``xstats``: RX/TX extended NIC statistics.

* ``fdir``: Flow Director information and statistics.

* ``stat_qmap``: Queue statistics mapping.

* ``dcb_tc``: DCB information such as TC mapping.

* ``cap``: Supported offload capabilities.

For example:

.. code-block:: console

   testpmd> show port info 0

   ********************* Infos for port 0 *********************

   MAC address: XX:XX:XX:XX:XX:XX
   Connect to socket: 0
   memory allocation on the socket: 0
   Link status: up
   Link speed: 40000 Mbps
   Link duplex: full-duplex
   Promiscuous mode: enabled
   Allmulticast mode: disabled
   Maximum number of MAC addresses: 64
   Maximum number of MAC addresses of hash filtering: 0
   VLAN offload:
       strip on
       filter on
       qinq(extend) off
   Redirection table size: 512
   Supported flow types:
     ipv4-frag
     ipv4-tcp
     ipv4-udp
     ipv4-sctp
     ipv4-other
     ipv6-frag
     ipv6-tcp
     ipv6-udp
     ipv6-sctp
     ipv6-other
     l2_payload
     port
     vxlan
     geneve
     nvgre

show port rss reta
~~~~~~~~~~~~~~~~~~

Display the rss redirection table entry indicated by masks on port X::

   testpmd> show port (port_id) rss reta (size) (mask0, mask1...)

size is used to indicate the hardware supported reta size

show port rss-hash
~~~~~~~~~~~~~~~~~~

Display the RSS hash functions and RSS hash key of a port::

   testpmd> show port (port_id) rss-hash ipv4|ipv4-frag|ipv4-tcp|ipv4-udp|ipv4-sctp|ipv4-other|ipv6|ipv6-frag|ipv6-tcp|ipv6-udp|ipv6-sctp|ipv6-other|l2-payload|ipv6-ex|ipv6-tcp-ex|ipv6-udp-ex [key]

clear port
~~~~~~~~~~

Clear the port statistics for a given port or for all ports::

   testpmd> clear port (info|stats|xstats|fdir|stat_qmap) (port_id|all)

For example::

   testpmd> clear port stats all

show (rxq|txq)
~~~~~~~~~~~~~~

Display information for a given port's RX/TX queue::

   testpmd> show (rxq|txq) info (port_id) (queue_id)

show config
~~~~~~~~~~~

Displays the configuration of the application.
The configuration comes from the command-line, the runtime or the application defaults::

   testpmd> show config (rxtx|cores|fwd|txpkts)

The available information categories are:

* ``rxtx``: RX/TX configuration items.

* ``cores``: List of forwarding cores.

* ``fwd``: Packet forwarding configuration.

* ``txpkts``: Packets to TX configuration.

For example:

.. code-block:: console

   testpmd> show config rxtx

   io packet forwarding - CRC stripping disabled - packets/burst=16
   nb forwarding cores=2 - nb forwarding ports=1
   RX queues=1 - RX desc=128 - RX free threshold=0
   RX threshold registers: pthresh=8 hthresh=8 wthresh=4
   TX queues=1 - TX desc=512 - TX free threshold=0
   TX threshold registers: pthresh=36 hthresh=0 wthresh=0
   TX RS bit threshold=0 - TXQ flags=0x0

set fwd
~~~~~~~

Set the packet forwarding mode::

   testpmd> set fwd (io|mac|macswap|flowgen| \
                     rxonly|txonly|csum|icmpecho) (""|retry)

``retry`` can be specified for forwarding engines except ``rx_only``.

The available information categories are:

* ``io``: Forwards packets "as-is" in I/O mode.
  This is the fastest possible forwarding operation as it does not access packets data.
  This is the default mode.

* ``mac``: Changes the source and the destination Ethernet addresses of packets before forwarding them.
  Default application behaviour is to set source Ethernet address to that of the transmitting interface, and destination
  address to a dummy value (set during init). The user may specify a target destination Ethernet address via the 'eth-peer' or
  'eth-peer-configfile' command-line options. It is not currently possible to specify a specific source Ethernet address.

* ``macswap``: MAC swap forwarding mode.
  Swaps the source and the destination Ethernet addresses of packets before forwarding them.

* ``flowgen``: Multi-flow generation mode.
  Originates a number of flows (with varying destination IP addresses), and terminate receive traffic.

* ``rxonly``: Receives packets but doesn't transmit them.

* ``txonly``: Generates and transmits packets without receiving any.

* ``csum``: Changes the checksum field with hardware or software methods depending on the offload flags on the packet.

* ``icmpecho``: Receives a burst of packets, lookup for IMCP echo requests and, if any, send back ICMP echo replies.

* ``ieee1588``: Demonstrate L2 IEEE1588 V2 PTP timestamping for RX and TX. Requires ``CONFIG_RTE_LIBRTE_IEEE1588=y``.

* ``tm``: Traffic Management forwarding mode
  Demonstrates the use of ethdev traffic management APIs and softnic PMD for
  QoS traffic management. In this mode, 5-level hierarchical QoS scheduler is
  available as an default option that can be enabled through CLI. The user can
  also modify the default hierarchy or specify the new hierarchy through CLI for
  implementing QoS scheduler.  Requires ``CONFIG_RTE_LIBRTE_PMD_SOFTNIC=y`` ``CONFIG_RTE_LIBRTE_SCHED=y``.

Example::

   testpmd> set fwd rxonly

   Set rxonly packet forwarding mode


read rxd
~~~~~~~~

Display an RX descriptor for a port RX queue::

   testpmd> read rxd (port_id) (queue_id) (rxd_id)

For example::

   testpmd> read rxd 0 0 4
        0x0000000B - 0x001D0180 / 0x0000000B - 0x001D0180

read txd
~~~~~~~~

Display a TX descriptor for a port TX queue::

   testpmd> read txd (port_id) (queue_id) (txd_id)

For example::

   testpmd> read txd 0 0 4
        0x00000001 - 0x24C3C440 / 0x000F0000 - 0x2330003C

ddp get list
~~~~~~~~~~~~

Get loaded dynamic device personalization (DDP) package info list::

   testpmd> ddp get list (port_id)

ddp get info
~~~~~~~~~~~~

Display information about dynamic device personalization (DDP) profile::

   testpmd> ddp get info (profile_path)

show vf stats
~~~~~~~~~~~~~

Display VF statistics::

   testpmd> show vf stats (port_id) (vf_id)

clear vf stats
~~~~~~~~~~~~~~

Reset VF statistics::

   testpmd> clear vf stats (port_id) (vf_id)

show port pctype mapping
~~~~~~~~~~~~~~~~~~~~~~~~

List all items from the pctype mapping table::

   testpmd> show port (port_id) pctype mapping


Configuration Functions
-----------------------

The testpmd application can be configured from the runtime as well as from the command-line.

This section details the available configuration functions that are available.

.. note::

   Configuration changes only become active when forwarding is started/restarted.

set default
~~~~~~~~~~~

Reset forwarding to the default configuration::

   testpmd> set default

set verbose
~~~~~~~~~~~

Set the debug verbosity level::

   testpmd> set verbose (level)

Currently the only available levels are 0 (silent except for error) and 1 (fully verbose).

set log
~~~~~~~

Set the log level for a log type::

	testpmd> set log global|(type) (level)

Where:

* ``type`` is the log name.

* ``level`` is the log level.

For example, to change the global log level::
	testpmd> set log global (level)

Regexes can also be used for type. To change log level of user1, user2 and user3::
	testpmd> set log user[1-3] (level)

set nbport
~~~~~~~~~~

Set the number of ports used by the application:

set nbport (num)

This is equivalent to the ``--nb-ports`` command-line option.

set nbcore
~~~~~~~~~~

Set the number of cores used by the application::

   testpmd> set nbcore (num)

This is equivalent to the ``--nb-cores`` command-line option.

.. note::

   The number of cores used must not be greater than number of ports used multiplied by the number of queues per port.

set coremask
~~~~~~~~~~~~

Set the forwarding cores hexadecimal mask::

   testpmd> set coremask (mask)

This is equivalent to the ``--coremask`` command-line option.

.. note::

   The master lcore is reserved for command line parsing only and cannot be masked on for packet forwarding.

set portmask
~~~~~~~~~~~~

Set the forwarding ports hexadecimal mask::

   testpmd> set portmask (mask)

This is equivalent to the ``--portmask`` command-line option.

set burst
~~~~~~~~~

Set number of packets per burst::

   testpmd> set burst (num)

This is equivalent to the ``--burst command-line`` option.

When retry is enabled, the transmit delay time and number of retries can also be set::

   testpmd> set burst tx delay (microseconds) retry (num)

set txpkts
~~~~~~~~~~

Set the length of each segment of the TX-ONLY packets or length of packet for FLOWGEN mode::

   testpmd> set txpkts (x[,y]*)

Where x[,y]* represents a CSV list of values, without white space.

set txsplit
~~~~~~~~~~~

Set the split policy for the TX packets, applicable for TX-ONLY and CSUM forwarding modes::

   testpmd> set txsplit (off|on|rand)

Where:

* ``off`` disable packet copy & split for CSUM mode.

* ``on`` split outgoing packet into multiple segments. Size of each segment
  and number of segments per packet is determined by ``set txpkts`` command
  (see above).

* ``rand`` same as 'on', but number of segments per each packet is a random value between 1 and total number of segments.

set corelist
~~~~~~~~~~~~

Set the list of forwarding cores::

   testpmd> set corelist (x[,y]*)

For example, to change the forwarding cores:

.. code-block:: console

   testpmd> set corelist 3,1
   testpmd> show config fwd

   io packet forwarding - ports=2 - cores=2 - streams=2 - NUMA support disabled
   Logical Core 3 (socket 0) forwards packets on 1 streams:
   RX P=0/Q=0 (socket 0) -> TX P=1/Q=0 (socket 0) peer=02:00:00:00:00:01
   Logical Core 1 (socket 0) forwards packets on 1 streams:
   RX P=1/Q=0 (socket 0) -> TX P=0/Q=0 (socket 0) peer=02:00:00:00:00:00

.. note::

   The cores are used in the same order as specified on the command line.

set portlist
~~~~~~~~~~~~

Set the list of forwarding ports::

   testpmd> set portlist (x[,y]*)

For example, to change the port forwarding:

.. code-block:: console

   testpmd> set portlist 0,2,1,3
   testpmd> show config fwd

   io packet forwarding - ports=4 - cores=1 - streams=4
   Logical Core 3 (socket 0) forwards packets on 4 streams:
   RX P=0/Q=0 (socket 0) -> TX P=2/Q=0 (socket 0) peer=02:00:00:00:00:01
   RX P=2/Q=0 (socket 0) -> TX P=0/Q=0 (socket 0) peer=02:00:00:00:00:00
   RX P=1/Q=0 (socket 0) -> TX P=3/Q=0 (socket 0) peer=02:00:00:00:00:03
   RX P=3/Q=0 (socket 0) -> TX P=1/Q=0 (socket 0) peer=02:00:00:00:00:02

set tx loopback
~~~~~~~~~~~~~~~

Enable/disable tx loopback::

   testpmd> set tx loopback (port_id) (on|off)

set drop enable
~~~~~~~~~~~~~~~

set drop enable bit for all queues::

   testpmd> set all queues drop (port_id) (on|off)

set split drop enable (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

set split drop enable bit for VF from PF::

   testpmd> set vf split drop (port_id) (vf_id) (on|off)

set mac antispoof (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~

Set mac antispoof for a VF from the PF::

   testpmd> set vf mac antispoof  (port_id) (vf_id) (on|off)

set macsec offload
~~~~~~~~~~~~~~~~~~

Enable/disable MACsec offload::

   testpmd> set macsec offload (port_id) on encrypt (on|off) replay-protect (on|off)
   testpmd> set macsec offload (port_id) off

set macsec sc
~~~~~~~~~~~~~

Configure MACsec secure connection (SC)::

   testpmd> set macsec sc (tx|rx) (port_id) (mac) (pi)

.. note::

   The pi argument is ignored for tx.
   Check the NIC Datasheet for hardware limits.

set macsec sa
~~~~~~~~~~~~~

Configure MACsec secure association (SA)::

   testpmd> set macsec sa (tx|rx) (port_id) (idx) (an) (pn) (key)

.. note::

   The IDX value must be 0 or 1.
   Check the NIC Datasheet for hardware limits.

set broadcast mode (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set broadcast mode for a VF from the PF::

   testpmd> set vf broadcast (port_id) (vf_id) (on|off)

vlan set strip
~~~~~~~~~~~~~~

Set the VLAN strip on a port::

   testpmd> vlan set strip (on|off) (port_id)

vlan set stripq
~~~~~~~~~~~~~~~

Set the VLAN strip for a queue on a port::

   testpmd> vlan set stripq (on|off) (port_id,queue_id)

vlan set stripq (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~

Set VLAN strip for all queues in a pool for a VF from the PF::

   testpmd> set vf vlan stripq (port_id) (vf_id) (on|off)

vlan set insert (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~

Set VLAN insert for a VF from the PF::

   testpmd> set vf vlan insert (port_id) (vf_id) (vlan_id)

vlan set tag (for VF)
~~~~~~~~~~~~~~~~~~~~~

Set VLAN tag for a VF from the PF::

   testpmd> set vf vlan tag (port_id) (vf_id) (on|off)

vlan set antispoof (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set VLAN antispoof for a VF from the PF::

   testpmd> set vf vlan antispoof (port_id) (vf_id) (on|off)

vlan set filter
~~~~~~~~~~~~~~~

Set the VLAN filter on a port::

   testpmd> vlan set filter (on|off) (port_id)

vlan set qinq
~~~~~~~~~~~~~

Set the VLAN QinQ (extended queue in queue) on for a port::

   testpmd> vlan set qinq (on|off) (port_id)

vlan set tpid
~~~~~~~~~~~~~

Set the inner or outer VLAN TPID for packet filtering on a port::

   testpmd> vlan set (inner|outer) tpid (value) (port_id)

.. note::

   TPID value must be a 16-bit number (value <= 65536).

rx_vlan add
~~~~~~~~~~~

Add a VLAN ID, or all identifiers, to the set of VLAN identifiers filtered by port ID::

   testpmd> rx_vlan add (vlan_id|all) (port_id)

.. note::

   VLAN filter must be set on that port. VLAN ID < 4096.
   Depending on the NIC used, number of vlan_ids may be limited to the maximum entries
   in VFTA table. This is important if enabling all vlan_ids.

rx_vlan rm
~~~~~~~~~~

Remove a VLAN ID, or all identifiers, from the set of VLAN identifiers filtered by port ID::

   testpmd> rx_vlan rm (vlan_id|all) (port_id)

rx_vlan add (for VF)
~~~~~~~~~~~~~~~~~~~~

Add a VLAN ID, to the set of VLAN identifiers filtered for VF(s) for port ID::

   testpmd> rx_vlan add (vlan_id) port (port_id) vf (vf_mask)

rx_vlan rm (for VF)
~~~~~~~~~~~~~~~~~~~

Remove a VLAN ID, from the set of VLAN identifiers filtered for VF(s) for port ID::

   testpmd> rx_vlan rm (vlan_id) port (port_id) vf (vf_mask)

tunnel_filter add
~~~~~~~~~~~~~~~~~

Add a tunnel filter on a port::

   testpmd> tunnel_filter add (port_id) (outer_mac) (inner_mac) (ip_addr) \
            (inner_vlan) (vxlan|nvgre|ipingre) (imac-ivlan|imac-ivlan-tenid|\
            imac-tenid|imac|omac-imac-tenid|oip|iip) (tenant_id) (queue_id)

The available information categories are:

* ``vxlan``: Set tunnel type as VXLAN.

* ``nvgre``: Set tunnel type as NVGRE.

* ``ipingre``: Set tunnel type as IP-in-GRE.

* ``imac-ivlan``: Set filter type as Inner MAC and VLAN.

* ``imac-ivlan-tenid``: Set filter type as Inner MAC, VLAN and tenant ID.

* ``imac-tenid``: Set filter type as Inner MAC and tenant ID.

* ``imac``: Set filter type as Inner MAC.

* ``omac-imac-tenid``: Set filter type as Outer MAC, Inner MAC and tenant ID.

* ``oip``: Set filter type as Outer IP.

* ``iip``: Set filter type as Inner IP.

Example::

   testpmd> tunnel_filter add 0 68:05:CA:28:09:82 00:00:00:00:00:00 \
            192.168.2.2 0 ipingre oip 1 1

   Set an IP-in-GRE tunnel on port 0, and the filter type is Outer IP.

tunnel_filter remove
~~~~~~~~~~~~~~~~~~~~

Remove a tunnel filter on a port::

   testpmd> tunnel_filter rm (port_id) (outer_mac) (inner_mac) (ip_addr) \
            (inner_vlan) (vxlan|nvgre|ipingre) (imac-ivlan|imac-ivlan-tenid|\
            imac-tenid|imac|omac-imac-tenid|oip|iip) (tenant_id) (queue_id)

rx_vxlan_port add
~~~~~~~~~~~~~~~~~

Add an UDP port for VXLAN packet filter on a port::

   testpmd> rx_vxlan_port add (udp_port) (port_id)

rx_vxlan_port remove
~~~~~~~~~~~~~~~~~~~~

Remove an UDP port for VXLAN packet filter on a port::

   testpmd> rx_vxlan_port rm (udp_port) (port_id)

tx_vlan set
~~~~~~~~~~~

Set hardware insertion of VLAN IDs in packets sent on a port::

   testpmd> tx_vlan set (port_id) vlan_id[, vlan_id_outer]

For example, set a single VLAN ID (5) insertion on port 0::

   tx_vlan set 0 5

Or, set double VLAN ID (inner: 2, outer: 3) insertion on port 1::

   tx_vlan set 1 2 3


tx_vlan set pvid
~~~~~~~~~~~~~~~~

Set port based hardware insertion of VLAN ID in packets sent on a port::

   testpmd> tx_vlan set pvid (port_id) (vlan_id) (on|off)

tx_vlan reset
~~~~~~~~~~~~~

Disable hardware insertion of a VLAN header in packets sent on a port::

   testpmd> tx_vlan reset (port_id)

csum set
~~~~~~~~

Select hardware or software calculation of the checksum when
transmitting a packet using the ``csum`` forwarding engine::

   testpmd> csum set (ip|udp|tcp|sctp|outer-ip) (hw|sw) (port_id)

Where:

* ``ip|udp|tcp|sctp`` always relate to  the inner layer.

* ``outer-ip`` relates to the outer IP layer (only for IPv4) in the case where the packet is recognized
  as a tunnel packet by the forwarding engine (vxlan, gre and ipip are
  supported). See also the ``csum parse-tunnel`` command.

.. note::

   Check the NIC Datasheet for hardware limits.

RSS queue region
~~~~~~~~~~~~~~~~

Set RSS queue region span on a port::

   testpmd> set port (port_id) queue-region region_id (value) \
		queue_start_index (value) queue_num (value)

Set flowtype mapping on a RSS queue region on a port::

   testpmd> set port (port_id) queue-region region_id (value) flowtype (value)

where:

* For the flowtype(pctype) of packet,the specific index for each type has
  been defined in file i40e_type.h as enum i40e_filter_pctype.

Set user priority mapping on a RSS queue region on a port::

   testpmd> set port (port_id) queue-region UP (value) region_id (value)

Flush all queue region related configuration on a port::

   testpmd> set port (port_id) queue-region flush (on|off)

where:

* "on"is just an enable function which server for other configuration,
  it is for all configuration about queue region from up layer,
  at first will only keep in DPDK softwarestored in driver,
  only after "flush on", it commit all configuration to HW.
  "off" is just clean all configuration about queue region just now,
  and restore all to DPDK i40e driver default config when start up.

Show all queue region related configuration info on a port::

   testpmd> show port (port_id) queue-region

.. note::

  Queue region only support on PF by now, so these command is
  only for configuration of queue region on PF port.

csum parse-tunnel
~~~~~~~~~~~~~~~~~

Define how tunneled packets should be handled by the csum forward
engine::

   testpmd> csum parse-tunnel (on|off) (tx_port_id)

If enabled, the csum forward engine will try to recognize supported
tunnel headers (vxlan, gre, ipip).

If disabled, treat tunnel packets as non-tunneled packets (a inner
header is handled as a packet payload).

.. note::

   The port argument is the TX port like in the ``csum set`` command.

Example:

Consider a packet in packet like the following::

   eth_out/ipv4_out/udp_out/vxlan/eth_in/ipv4_in/tcp_in

* If parse-tunnel is enabled, the ``ip|udp|tcp|sctp`` parameters of ``csum set``
  command relate to the inner headers (here ``ipv4_in`` and ``tcp_in``), and the
  ``outer-ip parameter`` relates to the outer headers (here ``ipv4_out``).

* If parse-tunnel is disabled, the ``ip|udp|tcp|sctp`` parameters of ``csum  set``
   command relate to the outer headers, here ``ipv4_out`` and ``udp_out``.

csum show
~~~~~~~~~

Display tx checksum offload configuration::

   testpmd> csum show (port_id)

tso set
~~~~~~~

Enable TCP Segmentation Offload (TSO) in the ``csum`` forwarding engine::

   testpmd> tso set (segsize) (port_id)

.. note::

   Check the NIC datasheet for hardware limits.

tso show
~~~~~~~~

Display the status of TCP Segmentation Offload::

   testpmd> tso show (port_id)

set port - gro
~~~~~~~~~~~~~~

Enable or disable GRO in ``csum`` forwarding engine::

   testpmd> set port <port_id> gro on|off

If enabled, the csum forwarding engine will perform GRO on the TCP/IPv4
packets received from the given port.

If disabled, packets received from the given port won't be performed
GRO. By default, GRO is disabled for all ports.

.. note::

   When enable GRO for a port, TCP/IPv4 packets received from the port
   will be performed GRO. After GRO, all merged packets have bad
   checksums, since the GRO library doesn't re-calculate checksums for
   the merged packets. Therefore, if users want the merged packets to
   have correct checksums, please select HW IP checksum calculation and
   HW TCP checksum calculation for the port which the merged packets are
   transmitted to.

show port - gro
~~~~~~~~~~~~~~~

Display GRO configuration for a given port::

   testpmd> show port <port_id> gro

set gro flush
~~~~~~~~~~~~~

Set the cycle to flush the GROed packets from reassembly tables::

   testpmd> set gro flush <cycles>

When enable GRO, the csum forwarding engine performs GRO on received
packets, and the GROed packets are stored in reassembly tables. Users
can use this command to determine when the GROed packets are flushed
from the reassembly tables.

The ``cycles`` is measured in GRO operation times. The csum forwarding
engine flushes the GROed packets from the tables every ``cycles`` GRO
operations.

By default, the value of ``cycles`` is 1, which means flush GROed packets
from the reassembly tables as soon as one GRO operation finishes. The value
of ``cycles`` should be in the range of 1 to ``GRO_MAX_FLUSH_CYCLES``.

Please note that the large value of ``cycles`` may cause the poor TCP/IP
stack performance. Because the GROed packets are delayed to arrive the
stack, thus causing more duplicated ACKs and TCP retransmissions.

set port - gso
~~~~~~~~~~~~~~

Toggle per-port GSO support in ``csum`` forwarding engine::

   testpmd> set port <port_id> gso on|off

If enabled, the csum forwarding engine will perform GSO on supported IPv4
packets, transmitted on the given port.

If disabled, packets transmitted on the given port will not undergo GSO.
By default, GSO is disabled for all ports.

.. note::

   When GSO is enabled on a port, supported IPv4 packets transmitted on that
   port undergo GSO. Afterwards, the segmented packets are represented by
   multi-segment mbufs; however, the csum forwarding engine doesn't calculation
   of checksums for GSO'd segments in SW. As a result, if users want correct
   checksums in GSO segments, they should enable HW checksum calculation for
   GSO-enabled ports.

   For example, HW checksum calculation for VxLAN GSO'd packets may be enabled
   by setting the following options in the csum forwarding engine:

   testpmd> csum set outer_ip hw <port_id>

   testpmd> csum set ip hw <port_id>

   testpmd> csum set tcp hw <port_id>

set gso segsz
~~~~~~~~~~~~~

Set the maximum GSO segment size (measured in bytes), which includes the
packet header and the packet payload for GSO-enabled ports (global)::

   testpmd> set gso segsz <length>

show port - gso
~~~~~~~~~~~~~~~

Display the status of Generic Segmentation Offload for a given port::

   testpmd> show port <port_id> gso

mac_addr add
~~~~~~~~~~~~

Add an alternative MAC address to a port::

   testpmd> mac_addr add (port_id) (XX:XX:XX:XX:XX:XX)

mac_addr remove
~~~~~~~~~~~~~~~

Remove a MAC address from a port::

   testpmd> mac_addr remove (port_id) (XX:XX:XX:XX:XX:XX)

mac_addr add (for VF)
~~~~~~~~~~~~~~~~~~~~~

Add an alternative MAC address for a VF to a port::

   testpmd> mac_add add port (port_id) vf (vf_id) (XX:XX:XX:XX:XX:XX)

mac_addr set
~~~~~~~~~~~~

Set the default MAC address for a port::

   testpmd> mac_addr set (port_id) (XX:XX:XX:XX:XX:XX)

mac_addr set (for VF)
~~~~~~~~~~~~~~~~~~~~~

Set the MAC address for a VF from the PF::

   testpmd> set vf mac addr (port_id) (vf_id) (XX:XX:XX:XX:XX:XX)

set eth-peer
~~~~~~~~~~~~

Set the forwarding peer address for certain port::

   testpmd> set eth-peer (port_id) (perr_addr)

This is equivalent to the ``--eth-peer`` command-line option.

set port-uta
~~~~~~~~~~~~

Set the unicast hash filter(s) on/off for a port::

   testpmd> set port (port_id) uta (XX:XX:XX:XX:XX:XX|all) (on|off)

set promisc
~~~~~~~~~~~

Set the promiscuous mode on for a port or for all ports.
In promiscuous mode packets are not dropped if they aren't for the specified MAC address::

   testpmd> set promisc (port_id|all) (on|off)

set allmulti
~~~~~~~~~~~~

Set the allmulti mode for a port or for all ports::

   testpmd> set allmulti (port_id|all) (on|off)

Same as the ifconfig (8) option. Controls how multicast packets are handled.

set promisc (for VF)
~~~~~~~~~~~~~~~~~~~~

Set the unicast promiscuous mode for a VF from PF.
It's supported by Intel i40e NICs now.
In promiscuous mode packets are not dropped if they aren't for the specified MAC address::

   testpmd> set vf promisc (port_id) (vf_id) (on|off)

set allmulticast (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~

Set the multicast promiscuous mode for a VF from PF.
It's supported by Intel i40e NICs now.
In promiscuous mode packets are not dropped if they aren't for the specified MAC address::

   testpmd> set vf allmulti (port_id) (vf_id) (on|off)

set tx max bandwidth (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set TX max absolute bandwidth (Mbps) for a VF from PF::

   testpmd> set vf tx max-bandwidth (port_id) (vf_id) (max_bandwidth)

set tc tx min bandwidth (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set all TCs' TX min relative bandwidth (%) for a VF from PF::

   testpmd> set vf tc tx min-bandwidth (port_id) (vf_id) (bw1, bw2, ...)

set tc tx max bandwidth (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set a TC's TX max absolute bandwidth (Mbps) for a VF from PF::

   testpmd> set vf tc tx max-bandwidth (port_id) (vf_id) (tc_no) (max_bandwidth)

set tc strict link priority mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set some TCs' strict link priority mode on a physical port::

   testpmd> set tx strict-link-priority (port_id) (tc_bitmap)

set tc tx min bandwidth
~~~~~~~~~~~~~~~~~~~~~~~

Set all TCs' TX min relative bandwidth (%) globally for all PF and VFs::

   testpmd> set tc tx min-bandwidth (port_id) (bw1, bw2, ...)

set flow_ctrl rx
~~~~~~~~~~~~~~~~

Set the link flow control parameter on a port::

   testpmd> set flow_ctrl rx (on|off) tx (on|off) (high_water) (low_water) \
            (pause_time) (send_xon) mac_ctrl_frame_fwd (on|off) \
	    autoneg (on|off) (port_id)

Where:

* ``high_water`` (integer): High threshold value to trigger XOFF.

* ``low_water`` (integer): Low threshold value to trigger XON.

* ``pause_time`` (integer): Pause quota in the Pause frame.

* ``send_xon`` (0/1): Send XON frame.

* ``mac_ctrl_frame_fwd``: Enable receiving MAC control frames.

* ``autoneg``: Change the auto-negotiation parameter.

set pfc_ctrl rx
~~~~~~~~~~~~~~~

Set the priority flow control parameter on a port::

   testpmd> set pfc_ctrl rx (on|off) tx (on|off) (high_water) (low_water) \
            (pause_time) (priority) (port_id)

Where:

* ``high_water`` (integer): High threshold value.

* ``low_water`` (integer): Low threshold value.

* ``pause_time`` (integer): Pause quota in the Pause frame.

* ``priority`` (0-7): VLAN User Priority.

set stat_qmap
~~~~~~~~~~~~~

Set statistics mapping (qmapping 0..15) for RX/TX queue on port::

   testpmd> set stat_qmap (tx|rx) (port_id) (queue_id) (qmapping)

For example, to set rx queue 2 on port 0 to mapping 5::

   testpmd>set stat_qmap rx 0 2 5

set xstats-hide-zero
~~~~~~~~~~~~~~~~~~~~

Set the option to hide zero values for xstats display::

	testpmd> set xstats-hide-zero on|off

.. note::

	By default, the zero values are displayed for xstats.

set port - rx/tx (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~

Set VF receive/transmit from a port::

   testpmd> set port (port_id) vf (vf_id) (rx|tx) (on|off)

set port - mac address filter (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add/Remove unicast or multicast MAC addr filter for a VF::

   testpmd> set port (port_id) vf (vf_id) (mac_addr) \
            (exact-mac|exact-mac-vlan|hashmac|hashmac-vlan) (on|off)

set port - rx mode(for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the VF receive mode of a port::

   testpmd> set port (port_id) vf (vf_id) \
            rxmode (AUPE|ROPE|BAM|MPE) (on|off)

The available receive modes are:

* ``AUPE``: Accepts untagged VLAN.

* ``ROPE``: Accepts unicast hash.

* ``BAM``: Accepts broadcast packets.

* ``MPE``: Accepts all multicast packets.

set port - tx_rate (for Queue)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set TX rate limitation for a queue on a port::

   testpmd> set port (port_id) queue (queue_id) rate (rate_value)

set port - tx_rate (for VF)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set TX rate limitation for queues in VF on a port::

   testpmd> set port (port_id) vf (vf_id) rate (rate_value) queue_mask (queue_mask)

set port - mirror rule
~~~~~~~~~~~~~~~~~~~~~~

Set pool or vlan type mirror rule for a port::

   testpmd> set port (port_id) mirror-rule (rule_id) \
            (pool-mirror-up|pool-mirror-down|vlan-mirror) \
            (poolmask|vlanid[,vlanid]*) dst-pool (pool_id) (on|off)

Set link mirror rule for a port::

   testpmd> set port (port_id) mirror-rule (rule_id) \
           (uplink-mirror|downlink-mirror) dst-pool (pool_id) (on|off)

For example to enable mirror traffic with vlan 0,1 to pool 0::

   set port 0 mirror-rule 0 vlan-mirror 0,1 dst-pool 0 on

reset port - mirror rule
~~~~~~~~~~~~~~~~~~~~~~~~

Reset a mirror rule for a port::

   testpmd> reset port (port_id) mirror-rule (rule_id)

set flush_rx
~~~~~~~~~~~~

Set the flush on RX streams before forwarding.
The default is flush ``on``.
Mainly used with PCAP drivers to turn off the default behavior of flushing the first 512 packets on RX streams::

   testpmd> set flush_rx off

set bypass mode
~~~~~~~~~~~~~~~

Set the bypass mode for the lowest port on bypass enabled NIC::

   testpmd> set bypass mode (normal|bypass|isolate) (port_id)

set bypass event
~~~~~~~~~~~~~~~~

Set the event required to initiate specified bypass mode for the lowest port on a bypass enabled::

   testpmd> set bypass event (timeout|os_on|os_off|power_on|power_off) \
            mode (normal|bypass|isolate) (port_id)

Where:

* ``timeout``: Enable bypass after watchdog timeout.

* ``os_on``: Enable bypass when OS/board is powered on.

* ``os_off``: Enable bypass when OS/board is powered off.

* ``power_on``: Enable bypass when power supply is turned on.

* ``power_off``: Enable bypass when power supply is turned off.


set bypass timeout
~~~~~~~~~~~~~~~~~~

Set the bypass watchdog timeout to ``n`` seconds where 0 = instant::

   testpmd> set bypass timeout (0|1.5|2|3|4|8|16|32)

show bypass config
~~~~~~~~~~~~~~~~~~

Show the bypass configuration for a bypass enabled NIC using the lowest port on the NIC::

   testpmd> show bypass config (port_id)

set link up
~~~~~~~~~~~

Set link up for a port::

   testpmd> set link-up port (port id)

set link down
~~~~~~~~~~~~~

Set link down for a port::

   testpmd> set link-down port (port id)

E-tag set
~~~~~~~~~

Enable E-tag insertion for a VF on a port::

   testpmd> E-tag set insertion on port-tag-id (value) port (port_id) vf (vf_id)

Disable E-tag insertion for a VF on a port::

   testpmd> E-tag set insertion off port (port_id) vf (vf_id)

Enable/disable E-tag stripping on a port::

   testpmd> E-tag set stripping (on|off) port (port_id)

Enable/disable E-tag based forwarding on a port::

   testpmd> E-tag set forwarding (on|off) port (port_id)

Add an E-tag forwarding filter on a port::

   testpmd> E-tag set filter add e-tag-id (value) dst-pool (pool_id) port (port_id)

Delete an E-tag forwarding filter on a port::
   testpmd> E-tag set filter del e-tag-id (value) port (port_id)

ddp add
~~~~~~~

Load a dynamic device personalization (DDP) profile and store backup profile::

   testpmd> ddp add (port_id) (profile_path[,backup_profile_path])

ddp del
~~~~~~~

Delete a dynamic device personalization profile and restore backup profile::

   testpmd> ddp del (port_id) (backup_profile_path)

ptype mapping
~~~~~~~~~~~~~

List all items from the ptype mapping table::

   testpmd> ptype mapping get (port_id) (valid_only)

Where:

* ``valid_only``: A flag indicates if only list valid items(=1) or all itemss(=0).

Replace a specific or a group of software defined ptype with a new one::

   testpmd> ptype mapping replace  (port_id) (target) (mask) (pkt_type)

where:

* ``target``: A specific software ptype or a mask to represent a group of software ptypes.

* ``mask``: A flag indicate if "target" is a specific software ptype(=0) or a ptype mask(=1).

* ``pkt_type``: The new software ptype to replace the old ones.

Update hardware defined ptype to software defined packet type mapping table::

   testpmd> ptype mapping update (port_id) (hw_ptype) (sw_ptype)

where:

* ``hw_ptype``: hardware ptype as the index of the ptype mapping table.

* ``sw_ptype``: software ptype as the value of the ptype mapping table.

Reset ptype mapping table::

   testpmd> ptype mapping reset (port_id)

Port Functions
--------------

The following sections show functions for configuring ports.

.. note::

   Port configuration changes only become active when forwarding is started/restarted.

port attach
~~~~~~~~~~~

Attach a port specified by pci address or virtual device args::

   testpmd> port attach (identifier)

To attach a new pci device, the device should be recognized by kernel first.
Then it should be moved under DPDK management.
Finally the port can be attached to testpmd.

For example, to move a pci device using ixgbe under DPDK management:

.. code-block:: console

   # Check the status of the available devices.
   ./usertools/dpdk-devbind.py --status

   Network devices using DPDK-compatible driver
   ============================================
   <none>

   Network devices using kernel driver
   ===================================
   0000:0a:00.0 '82599ES 10-Gigabit' if=eth2 drv=ixgbe unused=


   # Bind the device to igb_uio.
   sudo ./usertools/dpdk-devbind.py -b igb_uio 0000:0a:00.0


   # Recheck the status of the devices.
   ./usertools/dpdk-devbind.py --status
   Network devices using DPDK-compatible driver
   ============================================
   0000:0a:00.0 '82599ES 10-Gigabit' drv=igb_uio unused=

To attach a port created by virtual device, above steps are not needed.

For example, to attach a port whose pci address is 0000:0a:00.0.

.. code-block:: console

   testpmd> port attach 0000:0a:00.0
   Attaching a new port...
   EAL: PCI device 0000:0a:00.0 on NUMA socket -1
   EAL:   probe driver: 8086:10fb rte_ixgbe_pmd
   EAL:   PCI memory mapped at 0x7f83bfa00000
   EAL:   PCI memory mapped at 0x7f83bfa80000
   PMD: eth_ixgbe_dev_init(): MAC: 2, PHY: 18, SFP+: 5
   PMD: eth_ixgbe_dev_init(): port 0 vendorID=0x8086 deviceID=0x10fb
   Port 0 is attached. Now total ports is 1
   Done

For example, to attach a port created by pcap PMD.

.. code-block:: console

   testpmd> port attach net_pcap0
   Attaching a new port...
   PMD: Initializing pmd_pcap for net_pcap0
   PMD: Creating pcap-backed ethdev on numa socket 0
   Port 0 is attached. Now total ports is 1
   Done

In this case, identifier is ``net_pcap0``.
This identifier format is the same as ``--vdev`` format of DPDK applications.

For example, to re-attach a bonded port which has been previously detached,
the mode and slave parameters must be given.

.. code-block:: console

   testpmd> port attach net_bond_0,mode=0,slave=1
   Attaching a new port...
   EAL: Initializing pmd_bond for net_bond_0
   EAL: Create bonded device net_bond_0 on port 0 in mode 0 on socket 0.
   Port 0 is attached. Now total ports is 1
   Done


port detach
~~~~~~~~~~~

Detach a specific port::

   testpmd> port detach (port_id)

Before detaching a port, the port should be stopped and closed.

For example, to detach a pci device port 0.

.. code-block:: console

   testpmd> port stop 0
   Stopping ports...
   Done
   testpmd> port close 0
   Closing ports...
   Done

   testpmd> port detach 0
   Detaching a port...
   EAL: PCI device 0000:0a:00.0 on NUMA socket -1
   EAL:   remove driver: 8086:10fb rte_ixgbe_pmd
   EAL:   PCI memory unmapped at 0x7f83bfa00000
   EAL:   PCI memory unmapped at 0x7f83bfa80000
   Done


For example, to detach a virtual device port 0.

.. code-block:: console

   testpmd> port stop 0
   Stopping ports...
   Done
   testpmd> port close 0
   Closing ports...
   Done

   testpmd> port detach 0
   Detaching a port...
   PMD: Closing pcap ethdev on numa socket 0
   Port 'net_pcap0' is detached. Now total ports is 0
   Done

To remove a pci device completely from the system, first detach the port from testpmd.
Then the device should be moved under kernel management.
Finally the device can be removed using kernel pci hotplug functionality.

For example, to move a pci device under kernel management:

.. code-block:: console

   sudo ./usertools/dpdk-devbind.py -b ixgbe 0000:0a:00.0

   ./usertools/dpdk-devbind.py --status

   Network devices using DPDK-compatible driver
   ============================================
   <none>

   Network devices using kernel driver
   ===================================
   0000:0a:00.0 '82599ES 10-Gigabit' if=eth2 drv=ixgbe unused=igb_uio

To remove a port created by a virtual device, above steps are not needed.

port start
~~~~~~~~~~

Start all ports or a specific port::

   testpmd> port start (port_id|all)

port stop
~~~~~~~~~

Stop all ports or a specific port::

   testpmd> port stop (port_id|all)

port close
~~~~~~~~~~

Close all ports or a specific port::

   testpmd> port close (port_id|all)

port start/stop queue
~~~~~~~~~~~~~~~~~~~~~

Start/stop a rx/tx queue on a specific port::

   testpmd> port (port_id) (rxq|txq) (queue_id) (start|stop)

Only take effect when port is started.

port config - speed
~~~~~~~~~~~~~~~~~~~

Set the speed and duplex mode for all ports or a specific port::

   testpmd> port config (port_id|all) speed (10|100|1000|10000|25000|40000|50000|100000|auto) \
            duplex (half|full|auto)

port config - queues/descriptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set number of queues/descriptors for rxq, txq, rxd and txd::

   testpmd> port config all (rxq|txq|rxd|txd) (value)

This is equivalent to the ``--rxq``, ``--txq``, ``--rxd`` and ``--txd`` command-line options.

port config - max-pkt-len
~~~~~~~~~~~~~~~~~~~~~~~~~

Set the maximum packet length::

   testpmd> port config all max-pkt-len (value)

This is equivalent to the ``--max-pkt-len`` command-line option.

port config - CRC Strip
~~~~~~~~~~~~~~~~~~~~~~~

Set hardware CRC stripping on or off for all ports::

   testpmd> port config all crc-strip (on|off)

CRC stripping is on by default.

The ``off`` option is equivalent to the ``--disable-crc-strip`` command-line option.

port config - scatter
~~~~~~~~~~~~~~~~~~~~~~~

Set RX scatter mode on or off for all ports::

   testpmd> port config all scatter (on|off)

RX scatter mode is off by default.

The ``on`` option is equivalent to the ``--enable-scatter`` command-line option.

port config - RX Checksum
~~~~~~~~~~~~~~~~~~~~~~~~~

Set hardware RX checksum offload to on or off for all ports::

   testpmd> port config all rx-cksum (on|off)

Checksum offload is off by default.

The ``on`` option is equivalent to the ``--enable-rx-cksum`` command-line option.

port config - VLAN
~~~~~~~~~~~~~~~~~~

Set hardware VLAN on or off for all ports::

   testpmd> port config all hw-vlan (on|off)

Hardware VLAN is off by default.

The ``on`` option is equivalent to the ``--enable-hw-vlan`` command-line option.

port config - VLAN filter
~~~~~~~~~~~~~~~~~~~~~~~~~

Set hardware VLAN filter on or off for all ports::

   testpmd> port config all hw-vlan-filter (on|off)

Hardware VLAN filter is off by default.

The ``on`` option is equivalent to the ``--enable-hw-vlan-filter`` command-line option.

port config - VLAN strip
~~~~~~~~~~~~~~~~~~~~~~~~

Set hardware VLAN strip on or off for all ports::

   testpmd> port config all hw-vlan-strip (on|off)

Hardware VLAN strip is off by default.

The ``on`` option is equivalent to the ``--enable-hw-vlan-strip`` command-line option.

port config - VLAN extend
~~~~~~~~~~~~~~~~~~~~~~~~~

Set hardware VLAN extend on or off for all ports::

   testpmd> port config all hw-vlan-extend (on|off)

Hardware VLAN extend is off by default.

The ``on`` option is equivalent to the ``--enable-hw-vlan-extend`` command-line option.

port config - Drop Packets
~~~~~~~~~~~~~~~~~~~~~~~~~~

Set packet drop for packets with no descriptors on or off for all ports::

   testpmd> port config all drop-en (on|off)

Packet dropping for packets with no descriptors is off by default.

The ``on`` option is equivalent to the ``--enable-drop-en`` command-line option.

port config - RSS
~~~~~~~~~~~~~~~~~

Set the RSS (Receive Side Scaling) mode on or off::

   testpmd> port config all rss (all|ip|tcp|udp|sctp|ether|port|vxlan|geneve|nvgre|none)

RSS is on by default.

The ``none`` option is equivalent to the ``--disable-rss`` command-line option.

port config - RSS Reta
~~~~~~~~~~~~~~~~~~~~~~

Set the RSS (Receive Side Scaling) redirection table::

   testpmd> port config all rss reta (hash,queue)[,(hash,queue)]

port config - DCB
~~~~~~~~~~~~~~~~~

Set the DCB mode for an individual port::

   testpmd> port config (port_id) dcb vt (on|off) (traffic_class) pfc (on|off)

The traffic class should be 4 or 8.

port config - Burst
~~~~~~~~~~~~~~~~~~~

Set the number of packets per burst::

   testpmd> port config all burst (value)

This is equivalent to the ``--burst`` command-line option.

port config - Threshold
~~~~~~~~~~~~~~~~~~~~~~~

Set thresholds for TX/RX queues::

   testpmd> port config all (threshold) (value)

Where the threshold type can be:

* ``txpt:`` Set the prefetch threshold register of the TX rings, 0 <= value <= 255.

* ``txht:`` Set the host threshold register of the TX rings, 0 <= value <= 255.

* ``txwt:`` Set the write-back threshold register of the TX rings, 0 <= value <= 255.

* ``rxpt:`` Set the prefetch threshold register of the RX rings, 0 <= value <= 255.

* ``rxht:`` Set the host threshold register of the RX rings, 0 <= value <= 255.

* ``rxwt:`` Set the write-back threshold register of the RX rings, 0 <= value <= 255.

* ``txfreet:`` Set the transmit free threshold of the TX rings, 0 <= value <= txd.

* ``rxfreet:`` Set the transmit free threshold of the RX rings, 0 <= value <= rxd.

* ``txrst:`` Set the transmit RS bit threshold of TX rings, 0 <= value <= txd.

These threshold options are also available from the command-line.

port config - E-tag
~~~~~~~~~~~~~~~~~~~

Set the value of ether-type for E-tag::

   testpmd> port config (port_id|all) l2-tunnel E-tag ether-type (value)

Enable/disable the E-tag support::

   testpmd> port config (port_id|all) l2-tunnel E-tag (enable|disable)

port config pctype mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~

Reset pctype mapping table::

   testpmd> port config (port_id) pctype mapping reset

Update hardware defined pctype to software defined flow type mapping table::

   testpmd> port config (port_id) pctype mapping update (pctype_id_0[,pctype_id_1]*) (flow_type_id)

where:

* ``pctype_id_x``: hardware pctype id as index of bit in bitmask value of the pctype mapping table.

* ``flow_type_id``: software flow type id as the index of the pctype mapping table.

port config input set
~~~~~~~~~~~~~~~~~~~~~

Config RSS/FDIR/FDIR flexible payload input set for some pctype::
   testpmd> port config (port_id) pctype (pctype_id) \
            (hash_inset|fdir_inset|fdir_flx_inset) \
	    (get|set|clear) field (field_idx)

Clear RSS/FDIR/FDIR flexible payload input set for some pctype::
   testpmd> port config (port_id) pctype (pctype_id) \
            (hash_inset|fdir_inset|fdir_flx_inset) clear all

where:

* ``pctype_id``: hardware packet classification types.
* ``field_idx``: hardware field index.

Link Bonding Functions
----------------------

The Link Bonding functions make it possible to dynamically create and
manage link bonding devices from within testpmd interactive prompt.

create bonded device
~~~~~~~~~~~~~~~~~~~~

Create a new bonding device::

   testpmd> create bonded device (mode) (socket)

For example, to create a bonded device in mode 1 on socket 0::

   testpmd> create bonded 1 0
   created new bonded device (port X)

add bonding slave
~~~~~~~~~~~~~~~~~

Adds Ethernet device to a Link Bonding device::

   testpmd> add bonding slave (slave id) (port id)

For example, to add Ethernet device (port 6) to a Link Bonding device (port 10)::

   testpmd> add bonding slave 6 10


remove bonding slave
~~~~~~~~~~~~~~~~~~~~

Removes an Ethernet slave device from a Link Bonding device::

   testpmd> remove bonding slave (slave id) (port id)

For example, to remove Ethernet slave device (port 6) to a Link Bonding device (port 10)::

   testpmd> remove bonding slave 6 10

set bonding mode
~~~~~~~~~~~~~~~~

Set the Link Bonding mode of a Link Bonding device::

   testpmd> set bonding mode (value) (port id)

For example, to set the bonding mode of a Link Bonding device (port 10) to broadcast (mode 3)::

   testpmd> set bonding mode 3 10

set bonding primary
~~~~~~~~~~~~~~~~~~~

Set an Ethernet slave device as the primary device on a Link Bonding device::

   testpmd> set bonding primary (slave id) (port id)

For example, to set the Ethernet slave device (port 6) as the primary port of a Link Bonding device (port 10)::

   testpmd> set bonding primary 6 10

set bonding mac
~~~~~~~~~~~~~~~

Set the MAC address of a Link Bonding device::

   testpmd> set bonding mac (port id) (mac)

For example, to set the MAC address of a Link Bonding device (port 10) to 00:00:00:00:00:01::

   testpmd> set bonding mac 10 00:00:00:00:00:01

set bonding xmit_balance_policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the transmission policy for a Link Bonding device when it is in Balance XOR mode::

   testpmd> set bonding xmit_balance_policy (port_id) (l2|l23|l34)

For example, set a Link Bonding device (port 10) to use a balance policy of layer 3+4 (IP addresses & UDP ports)::

   testpmd> set bonding xmit_balance_policy 10 l34


set bonding mon_period
~~~~~~~~~~~~~~~~~~~~~~

Set the link status monitoring polling period in milliseconds for a bonding device.

This adds support for PMD slave devices which do not support link status interrupts.
When the mon_period is set to a value greater than 0 then all PMD's which do not support
link status ISR will be queried every polling interval to check if their link status has changed::

   testpmd> set bonding mon_period (port_id) (value)

For example, to set the link status monitoring polling period of bonded device (port 5) to 150ms::

   testpmd> set bonding mon_period 5 150


set bonding lacp dedicated_queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enable dedicated tx/rx queues on bonding devices slaves to handle LACP control plane traffic
when in mode 4 (link-aggregration-802.3ad)::

   testpmd> set bonding lacp dedicated_queues (port_id) (enable|disable)


set bonding agg_mode
~~~~~~~~~~~~~~~~~~~~

Enable one of the specific aggregators mode when in mode 4 (link-aggregration-802.3ad)::

   testpmd> set bonding agg_mode (port_id) (bandwidth|count|stable)


show bonding config
~~~~~~~~~~~~~~~~~~~

Show the current configuration of a Link Bonding device::

   testpmd> show bonding config (port id)

For example,
to show the configuration a Link Bonding device (port 9) with 3 slave devices (1, 3, 4)
in balance mode with a transmission policy of layer 2+3::

   testpmd> show bonding config 9
        Bonding mode: 2
        Balance Xmit Policy: BALANCE_XMIT_POLICY_LAYER23
        Slaves (3): [1 3 4]
        Active Slaves (3): [1 3 4]
        Primary: [3]


Register Functions
------------------

The Register Functions can be used to read from and write to registers on the network card referenced by a port number.
This is mainly useful for debugging purposes.
Reference should be made to the appropriate datasheet for the network card for details on the register addresses
and fields that can be accessed.

read reg
~~~~~~~~

Display the value of a port register::

   testpmd> read reg (port_id) (address)

For example, to examine the Flow Director control register (FDIRCTL, 0x0000EE000) on an Intel 82599 10 GbE Controller::

   testpmd> read reg 0 0xEE00
   port 0 PCI register at offset 0xEE00: 0x4A060029 (1241907241)

read regfield
~~~~~~~~~~~~~

Display a port register bit field::

   testpmd> read regfield (port_id) (address) (bit_x) (bit_y)

For example, reading the lowest two bits from the register in the example above::

   testpmd> read regfield 0 0xEE00 0 1
   port 0 PCI register at offset 0xEE00: bits[0, 1]=0x1 (1)

read regbit
~~~~~~~~~~~

Display a single port register bit::

   testpmd> read regbit (port_id) (address) (bit_x)

For example, reading the lowest bit from the register in the example above::

   testpmd> read regbit 0 0xEE00 0
   port 0 PCI register at offset 0xEE00: bit 0=1

write reg
~~~~~~~~~

Set the value of a port register::

   testpmd> write reg (port_id) (address) (value)

For example, to clear a register::

   testpmd> write reg 0 0xEE00 0x0
   port 0 PCI register at offset 0xEE00: 0x00000000 (0)

write regfield
~~~~~~~~~~~~~~

Set bit field of a port register::

   testpmd> write regfield (port_id) (address) (bit_x) (bit_y) (value)

For example, writing to the register cleared in the example above::

   testpmd> write regfield 0 0xEE00 0 1 2
   port 0 PCI register at offset 0xEE00: 0x00000002 (2)

write regbit
~~~~~~~~~~~~

Set single bit value of a port register::

   testpmd> write regbit (port_id) (address) (bit_x) (value)

For example, to set the high bit in the register from the example above::

   testpmd> write regbit 0 0xEE00 31 1
   port 0 PCI register at offset 0xEE00: 0x8000000A (2147483658)

Traffic Metering and Policing
-----------------------------

The following section shows functions for configuring traffic metering and
policing on the ethernet device through the use of generic ethdev API.

show port traffic management capability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show traffic metering and policing capability of the port::

   testpmd> show port meter cap (port_id)

add port meter profile (srTCM rfc2967)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add meter profile (srTCM rfc2697) to the ethernet device::

   testpmd> add port meter profile srtcm_rfc2697 (port_id) (profile_id) \
   (cir) (cbs) (ebs)

where:

* ``profile_id``: ID for the meter profile.
* ``cir``: Committed Information Rate (CIR) (bytes/second).
* ``cbs``: Committed Burst Size (CBS) (bytes).
* ``ebs``: Excess Burst Size (EBS) (bytes).

add port meter profile (trTCM rfc2968)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add meter profile (srTCM rfc2698) to the ethernet device::

   testpmd> add port meter profile trtcm_rfc2698 (port_id) (profile_id) \
   (cir) (pir) (cbs) (pbs)

where:

* ``profile_id``: ID for the meter profile.
* ``cir``: Committed information rate (bytes/second).
* ``pir``: Peak information rate (bytes/second).
* ``cbs``: Committed burst size (bytes).
* ``pbs``: Peak burst size (bytes).

add port meter profile (trTCM rfc4115)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add meter profile (trTCM rfc4115) to the ethernet device::

   testpmd> add port meter profile trtcm_rfc4115 (port_id) (profile_id) \
   (cir) (eir) (cbs) (ebs)

where:

* ``profile_id``: ID for the meter profile.
* ``cir``: Committed information rate (bytes/second).
* ``eir``: Excess information rate (bytes/second).
* ``cbs``: Committed burst size (bytes).
* ``ebs``: Excess burst size (bytes).

delete port meter profile
~~~~~~~~~~~~~~~~~~~~~~~~~

Delete meter profile from the ethernet device::

   testpmd> del port meter profile (port_id) (profile_id)

create port meter
~~~~~~~~~~~~~~~~~

Create new meter object for the ethernet device::

   testpmd> create port meter (port_id) (mtr_id) (profile_id) \
   (meter_enable) (g_action) (y_action) (r_action) (stats_mask) (shared) \
   (use_pre_meter_color) [(dscp_tbl_entry0) (dscp_tbl_entry1)...\
   (dscp_tbl_entry63)]

where:

* ``mtr_id``: meter object ID.
* ``profile_id``: ID for the meter profile.
* ``meter_enable``: When this parameter has a non-zero value, the meter object
  gets enabled at the time of creation, otherwise remains disabled.
* ``g_action``: Policer action for the packet with green color.
* ``y_action``: Policer action for the packet with yellow color.
* ``r_action``: Policer action for the packet with red color.
* ``stats_mask``: Mask of statistics counter types to be enabled for the
  meter object.
* ``shared``:  When this parameter has a non-zero value, the meter object is
  shared by multiple flows. Otherwise, meter object is used by single flow.
* ``use_pre_meter_color``: When this parameter has a non-zero value, the
  input color for the current meter object is determined by the latest meter
  object in the same flow. Otherwise, the current meter object uses the
  *dscp_table* to determine the input color.
* ``dscp_tbl_entryx``: DSCP table entry x providing meter providing input
  color, 0 <= x <= 63.

enable port meter
~~~~~~~~~~~~~~~~~

Enable meter for the ethernet device::

   testpmd> enable port meter (port_id) (mtr_id)

disable port meter
~~~~~~~~~~~~~~~~~~

Disable meter for the ethernet device::

   testpmd> disable port meter (port_id) (mtr_id)

delete port meter
~~~~~~~~~~~~~~~~~

Delete meter for the ethernet device::

   testpmd> del port meter (port_id) (mtr_id)

Set port meter profile
~~~~~~~~~~~~~~~~~~~~~~

Set meter profile for the ethernet device::

   testpmd> set port meter profile (port_id) (mtr_id) (profile_id)

set port meter dscp table
~~~~~~~~~~~~~~~~~~~~~~~~~

Set meter dscp table for the ethernet device::

   testpmd> set port meter dscp table (port_id) (mtr_id) [(dscp_tbl_entry0) \
   (dscp_tbl_entry1)...(dscp_tbl_entry63)]

set port meter policer action
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set meter policer action for the ethernet device::

   testpmd> set port meter policer action (port_id) (mtr_id) (action_mask) \
   (action0) [(action1) (action1)]

where:

* ``action_mask``: Bit mask indicating which policer actions need to be
  updated. One or more policer actions can be updated in a single function
  invocation. To update the policer action associated with color C, bit
  (1 << C) needs to be set in *action_mask* and element at position C
  in the *actions* array needs to be valid.
* ``actionx``: Policer action for the color x,
  RTE_MTR_GREEN <= x < RTE_MTR_COLORS

set port meter stats mask
~~~~~~~~~~~~~~~~~~~~~~~~~

Set meter stats mask for the ethernet device::

   testpmd> set port meter stats mask (port_id) (mtr_id) (stats_mask)

where:

* ``stats_mask``: Bit mask indicating statistics counter types to be enabled.

show port meter stats
~~~~~~~~~~~~~~~~~~~~~

Show meter stats of the ethernet device::

   testpmd> show port meter stats (port_id) (mtr_id) (clear)

where:

* ``clear``: Flag that indicates whether the statistics counters should
  be cleared (i.e. set to zero) immediately after they have been read or not.

Traffic Management
------------------

The following section shows functions for configuring traffic management on
on the ethernet device through the use of generic TM API.

show port traffic management capability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show traffic management capability of the port::

   testpmd> show port tm cap (port_id)

show port traffic management capability (hierarchy level)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show traffic management hierarchy level capability of the port::

   testpmd> show port tm level cap (port_id) (level_id)

show port traffic management capability (hierarchy node level)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show the traffic management hierarchy node capability of the port::

   testpmd> show port tm node cap (port_id) (node_id)

show port traffic management hierarchy node type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show the port traffic management hierarchy node type::

   testpmd> show port tm node type (port_id) (node_id)

show port traffic management hierarchy node stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Show the port traffic management hierarchy node statistics::

   testpmd> show port tm node stats (port_id) (node_id) (clear)

where:

* ``clear``: When this parameter has a non-zero value, the statistics counters
  are cleared (i.e. set to zero) immediately after they have been read,
  otherwise the statistics counters are left untouched.

Add port traffic management private shaper profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add the port traffic management private shaper profile::

   testpmd> add port tm node shaper profile (port_id) (shaper_profile_id) \
   (tb_rate) (tb_size) (packet_length_adjust)

where:

* ``shaper_profile id``: Shaper profile ID for the new profile.
* ``tb_rate``: Token bucket rate (bytes per second).
* ``tb_size``: Token bucket size (bytes).
* ``packet_length_adjust``: The value (bytes) to be added to the length of
  each packet for the purpose of shaping. This parameter value can be used to
  correct the packet length with the framing overhead bytes that are consumed
  on the wire.

Delete port traffic management private shaper profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete the port traffic management private shaper::

   testpmd> del port tm node shaper profile (port_id) (shaper_profile_id)

where:

* ``shaper_profile id``: Shaper profile ID that needs to be deleted.

Add port traffic management shared shaper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the port traffic management shared shaper::

   testpmd> add port tm node shared shaper (port_id) (shared_shaper_id) \
   (shaper_profile_id)

where:

* ``shared_shaper_id``: Shared shaper ID to be created.
* ``shaper_profile id``: Shaper profile ID for shared shaper.

Set port traffic management shared shaper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update the port traffic management shared shaper::

   testpmd> set port tm node shared shaper (port_id) (shared_shaper_id) \
   (shaper_profile_id)

where:

* ``shared_shaper_id``: Shared shaper ID to be update.
* ``shaper_profile id``: Shaper profile ID for shared shaper.

Delete port traffic management shared shaper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete the port traffic management shared shaper::

   testpmd> del port tm node shared shaper (port_id) (shared_shaper_id)

where:

* ``shared_shaper_id``: Shared shaper ID to be deleted.

Set port traffic management hiearchy node private shaper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

set the port traffic management hierarchy node private shaper::

   testpmd> set port tm node shaper profile (port_id) (node_id) \
   (shaper_profile_id)

where:

* ``shaper_profile id``: Private shaper profile ID to be enabled on the
  hierarchy node.

Add port traffic management WRED profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new WRED profile::

   testpmd> add port tm node wred profile (port_id) (wred_profile_id) \
   (color_g) (min_th_g) (max_th_g) (maxp_inv_g) (wq_log2_g) \
   (color_y) (min_th_y) (max_th_y) (maxp_inv_y) (wq_log2_y) \
   (color_r) (min_th_r) (max_th_r) (maxp_inv_r) (wq_log2_r)

where:

* ``wred_profile id``: Identifier for the newly create WRED profile
* ``color_g``: Packet color (green)
* ``min_th_g``: Minimum queue threshold for packet with green color
* ``max_th_g``: Minimum queue threshold for packet with green color
* ``maxp_inv_g``: Inverse of packet marking probability maximum value (maxp)
* ``wq_log2_g``: Negated log2 of queue weight (wq)
* ``color_y``: Packet color (yellow)
* ``min_th_y``: Minimum queue threshold for packet with yellow color
* ``max_th_y``: Minimum queue threshold for packet with yellow color
* ``maxp_inv_y``: Inverse of packet marking probability maximum value (maxp)
* ``wq_log2_y``: Negated log2 of queue weight (wq)
* ``color_r``: Packet color (red)
* ``min_th_r``: Minimum queue threshold for packet with yellow color
* ``max_th_r``: Minimum queue threshold for packet with yellow color
* ``maxp_inv_r``: Inverse of packet marking probability maximum value (maxp)
* ``wq_log2_r``: Negated log2 of queue weight (wq)

Delete port traffic management WRED profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete the WRED profile::

   testpmd> del port tm node wred profile (port_id) (wred_profile_id)

Add port traffic management hierarchy nonleaf node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add nonleaf node to port traffic management hiearchy::

   testpmd> add port tm nonleaf node (port_id) (node_id) (parent_node_id) \
   (priority) (weight) (level_id) (shaper_profile_id) \
   (n_sp_priorities) (stats_mask) (n_shared_shapers) \
   [(shared_shaper_0) (shared_shaper_1) ...] \

where:

* ``parent_node_id``: Node ID of the parent.
* ``priority``: Node priority (highest node priority is zero). This is used by
  the SP algorithm running on the parent node for scheduling this node.
* ``weight``: Node weight (lowest weight is one). The node weight is relative
  to the weight sum of all siblings that have the same priority. It is used by
  the WFQ algorithm running on the parent node for scheduling this node.
* ``level_id``: Hiearchy level of the node.
* ``shaper_profile_id``: Shaper profile ID of the private shaper to be used by
  the node.
* ``n_sp_priorities``: Number of strict priorities.
* ``stats_mask``: Mask of statistics counter types to be enabled for this node.
* ``n_shared_shapers``: Number of shared shapers.
* ``shared_shaper_id``: Shared shaper id.

Add port traffic management hierarchy leaf node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add leaf node to port traffic management hiearchy::

   testpmd> add port tm leaf node (port_id) (node_id) (parent_node_id) \
   (priority) (weight) (level_id) (shaper_profile_id) \
   (cman_mode) (wred_profile_id) (stats_mask) (n_shared_shapers) \
   [(shared_shaper_id) (shared_shaper_id) ...] \

where:

* ``parent_node_id``: Node ID of the parent.
* ``priority``: Node priority (highest node priority is zero). This is used by
  the SP algorithm running on the parent node for scheduling this node.
* ``weight``: Node weight (lowest weight is one). The node weight is relative
  to the weight sum of all siblings that have the same priority. It is used by
  the WFQ algorithm running on the parent node for scheduling this node.
* ``level_id``: Hiearchy level of the node.
* ``shaper_profile_id``: Shaper profile ID of the private shaper to be used by
  the node.
* ``cman_mode``: Congestion management mode to be enabled for this node.
* ``wred_profile_id``: WRED profile id to be enabled for this node.
* ``stats_mask``: Mask of statistics counter types to be enabled for this node.
* ``n_shared_shapers``: Number of shared shapers.
* ``shared_shaper_id``: Shared shaper id.

Delete port traffic management hierarchy node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete node from port traffic management hiearchy::

   testpmd> del port tm node (port_id) (node_id)

Update port traffic management hierarchy parent node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update port traffic management hierarchy parent node::

   testpmd> set port tm node parent (port_id) (node_id) (parent_node_id) \
   (priority) (weight)

This function can only be called after the hierarchy commit invocation. Its
success depends on the port support for this operation, as advertised through
the port capability set. This function is valid for all nodes of the traffic
management hierarchy except root node.

Commit port traffic management hierarchy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Commit the traffic management hierarchy on the port::

   testpmd> port tm hierarchy commit (port_id) (clean_on_fail)

where:

* ``clean_on_fail``: When set to non-zero, hierarchy is cleared on function
  call failure. On the other hand, hierarchy is preserved when this parameter
  is equal to zero.

Set port traffic management default hierarchy (tm forwarding mode)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

set the traffic management default hierarchy on the port::

   testpmd> set port tm hierarchy default (port_id)

Filter Functions
----------------

This section details the available filter functions that are available.

Note these functions interface the deprecated legacy filtering framework,
superseded by *rte_flow*. See `Flow rules management`_.

ethertype_filter
~~~~~~~~~~~~~~~~~~~~

Add or delete a L2 Ethertype filter, which identify packets by their L2 Ethertype mainly assign them to a receive queue::

   ethertype_filter (port_id) (add|del) (mac_addr|mac_ignr) (mac_address) \
                    ethertype (ether_type) (drop|fwd) queue (queue_id)

The available information parameters are:

* ``port_id``: The port which the Ethertype filter assigned on.

* ``mac_addr``: Compare destination mac address.

* ``mac_ignr``: Ignore destination mac address match.

* ``mac_address``: Destination mac address to match.

* ``ether_type``: The EtherType value want to match,
  for example 0x0806 for ARP packet. 0x0800 (IPv4) and 0x86DD (IPv6) are invalid.

* ``queue_id``: The receive queue associated with this EtherType filter.
  It is meaningless when deleting or dropping.

Example, to add/remove an ethertype filter rule::

   testpmd> ethertype_filter 0 add mac_ignr 00:11:22:33:44:55 \
                             ethertype 0x0806 fwd queue 3

   testpmd> ethertype_filter 0 del mac_ignr 00:11:22:33:44:55 \
                             ethertype 0x0806 fwd queue 3

2tuple_filter
~~~~~~~~~~~~~~~~~

Add or delete a 2-tuple filter,
which identifies packets by specific protocol and destination TCP/UDP port
and forwards packets into one of the receive queues::

   2tuple_filter (port_id) (add|del) dst_port (dst_port_value) \
                 protocol (protocol_value) mask (mask_value) \
                 tcp_flags (tcp_flags_value) priority (prio_value) \
                 queue (queue_id)

The available information parameters are:

* ``port_id``: The port which the 2-tuple filter assigned on.

* ``dst_port_value``: Destination port in L4.

* ``protocol_value``: IP L4 protocol.

* ``mask_value``: Participates in the match or not by bit for field above, 1b means participate.

* ``tcp_flags_value``: TCP control bits. The non-zero value is invalid, when the pro_value is not set to 0x06 (TCP).

* ``prio_value``: Priority of this filter.

* ``queue_id``: The receive queue associated with this 2-tuple filter.

Example, to add/remove an 2tuple filter rule::

   testpmd> 2tuple_filter 0 add dst_port 32 protocol 0x06 mask 0x03 \
                          tcp_flags 0x02 priority 3 queue 3

   testpmd> 2tuple_filter 0 del dst_port 32 protocol 0x06 mask 0x03 \
                          tcp_flags 0x02 priority 3 queue 3

5tuple_filter
~~~~~~~~~~~~~~~~~

Add or delete a 5-tuple filter,
which consists of a 5-tuple (protocol, source and destination IP addresses, source and destination TCP/UDP/SCTP port)
and routes packets into one of the receive queues::

   5tuple_filter (port_id) (add|del) dst_ip (dst_address) src_ip \
                 (src_address) dst_port (dst_port_value) \
                 src_port (src_port_value) protocol (protocol_value) \
                 mask (mask_value) tcp_flags (tcp_flags_value) \
                 priority (prio_value) queue (queue_id)

The available information parameters are:

* ``port_id``: The port which the 5-tuple filter assigned on.

* ``dst_address``: Destination IP address.

* ``src_address``: Source IP address.

* ``dst_port_value``: TCP/UDP destination port.

* ``src_port_value``: TCP/UDP source port.

* ``protocol_value``: L4 protocol.

* ``mask_value``: Participates in the match or not by bit for field above, 1b means participate

* ``tcp_flags_value``: TCP control bits. The non-zero value is invalid, when the protocol_value is not set to 0x06 (TCP).

* ``prio_value``: The priority of this filter.

* ``queue_id``: The receive queue associated with this 5-tuple filter.

Example, to add/remove an 5tuple filter rule::

   testpmd> 5tuple_filter 0 add dst_ip 2.2.2.5 src_ip 2.2.2.4 \
            dst_port 64 src_port 32 protocol 0x06 mask 0x1F \
            flags 0x0 priority 3 queue 3

   testpmd> 5tuple_filter 0 del dst_ip 2.2.2.5 src_ip 2.2.2.4 \
            dst_port 64 src_port 32 protocol 0x06 mask 0x1F \
            flags 0x0 priority 3 queue 3

syn_filter
~~~~~~~~~~

Using the  SYN filter, TCP packets whose *SYN* flag is set can be forwarded to a separate queue::

   syn_filter (port_id) (add|del) priority (high|low) queue (queue_id)

The available information parameters are:

* ``port_id``: The port which the SYN filter assigned on.

* ``high``: This SYN filter has higher priority than other filters.

* ``low``: This SYN filter has lower priority than other filters.

* ``queue_id``: The receive queue associated with this SYN filter

Example::

   testpmd> syn_filter 0 add priority high queue 3

flex_filter
~~~~~~~~~~~

With flex filter, packets can be recognized by any arbitrary pattern within the first 128 bytes of the packet
and routed into one of the receive queues::

   flex_filter (port_id) (add|del) len (len_value) bytes (bytes_value) \
               mask (mask_value) priority (prio_value) queue (queue_id)

The available information parameters are:

* ``port_id``: The port which the Flex filter is assigned on.

* ``len_value``: Filter length in bytes, no greater than 128.

* ``bytes_value``: A string in hexadecimal, means the value the flex filter needs to match.

* ``mask_value``: A string in hexadecimal, bit 1 means corresponding byte participates in the match.

* ``prio_value``: The priority of this filter.

* ``queue_id``: The receive queue associated with this Flex filter.

Example::

   testpmd> flex_filter 0 add len 16 bytes 0x00000000000000000000000008060000 \
                          mask 000C priority 3 queue 3

   testpmd> flex_filter 0 del len 16 bytes 0x00000000000000000000000008060000 \
                          mask 000C priority 3 queue 3


.. _testpmd_flow_director:

flow_director_filter
~~~~~~~~~~~~~~~~~~~~

The Flow Director works in receive mode to identify specific flows or sets of flows and route them to specific queues.

Four types of filtering are supported which are referred to as Perfect Match, Signature, Perfect-mac-vlan and
Perfect-tunnel filters, the match mode is set by the ``--pkt-filter-mode`` command-line parameter:

* Perfect match filters.
  The hardware checks a match between the masked fields of the received packets and the programmed filters.
  The masked fields are for IP flow.

* Signature filters.
  The hardware checks a match between a hash-based signature of the masked fields of the received packet.

* Perfect-mac-vlan match filters.
  The hardware checks a match between the masked fields of the received packets and the programmed filters.
  The masked fields are for MAC VLAN flow.

* Perfect-tunnel match filters.
  The hardware checks a match between the masked fields of the received packets and the programmed filters.
  The masked fields are for tunnel flow.

* Perfect-raw-flow-type match filters.
  The hardware checks a match between the masked fields of the received packets and pre-loaded raw (template) packet.
  The masked fields are specified by input sets.

The Flow Director filters can match the different fields for different type of packet: flow type, specific input set
per flow type and the flexible payload.

The Flow Director can also mask out parts of all of these fields so that filters
are only applied to certain fields or parts of the fields.

Note that for raw flow type mode the source and destination fields in the
raw packet buffer need to be presented in a reversed order with respect
to the expected received packets.
For example: IP source and destination addresses or TCP/UDP/SCTP
source and destination ports

Different NICs may have different capabilities, command show port fdir (port_id) can be used to acquire the information.

# Commands to add flow director filters of different flow types::

   flow_director_filter (port_id) mode IP (add|del|update) \
                        flow (ipv4-other|ipv4-frag|ipv6-other|ipv6-frag) \
                        src (src_ip_address) dst (dst_ip_address) \
                        tos (tos_value) proto (proto_value) ttl (ttl_value) \
                        vlan (vlan_value) flexbytes (flexbytes_value) \
                        (drop|fwd) pf|vf(vf_id) queue (queue_id) \
                        fd_id (fd_id_value)

   flow_director_filter (port_id) mode IP (add|del|update) \
                        flow (ipv4-tcp|ipv4-udp|ipv6-tcp|ipv6-udp) \
                        src (src_ip_address) (src_port) \
                        dst (dst_ip_address) (dst_port) \
                        tos (tos_value) ttl (ttl_value) \
                        vlan (vlan_value) flexbytes (flexbytes_value) \
                        (drop|fwd) queue pf|vf(vf_id) (queue_id) \
                        fd_id (fd_id_value)

   flow_director_filter (port_id) mode IP (add|del|update) \
                        flow (ipv4-sctp|ipv6-sctp) \
                        src (src_ip_address) (src_port) \
                        dst (dst_ip_address) (dst_port) \
                        tos (tos_value) ttl (ttl_value) \
                        tag (verification_tag) vlan (vlan_value) \
                        flexbytes (flexbytes_value) (drop|fwd) \
                        pf|vf(vf_id) queue (queue_id) fd_id (fd_id_value)

   flow_director_filter (port_id) mode IP (add|del|update) flow l2_payload \
                        ether (ethertype) flexbytes (flexbytes_value) \
                        (drop|fwd) pf|vf(vf_id) queue (queue_id)
                        fd_id (fd_id_value)

   flow_director_filter (port_id) mode MAC-VLAN (add|del|update) \
                        mac (mac_address) vlan (vlan_value) \
                        flexbytes (flexbytes_value) (drop|fwd) \
                        queue (queue_id) fd_id (fd_id_value)

   flow_director_filter (port_id) mode Tunnel (add|del|update) \
                        mac (mac_address) vlan (vlan_value) \
                        tunnel (NVGRE|VxLAN) tunnel-id (tunnel_id_value) \
                        flexbytes (flexbytes_value) (drop|fwd) \
                        queue (queue_id) fd_id (fd_id_value)

   flow_director_filter (port_id) mode raw (add|del|update) flow (flow_id) \
                        (drop|fwd) queue (queue_id) fd_id (fd_id_value) \
                        packet (packet file name)

For example, to add an ipv4-udp flow type filter::

   testpmd> flow_director_filter 0 mode IP add flow ipv4-udp src 2.2.2.3 32 \
            dst 2.2.2.5 33 tos 2 ttl 40 vlan 0x1 flexbytes (0x88,0x48) \
            fwd pf queue 1 fd_id 1

For example, add an ipv4-other flow type filter::

   testpmd> flow_director_filter 0 mode IP add flow ipv4-other src 2.2.2.3 \
             dst 2.2.2.5 tos 2 proto 20 ttl 40 vlan 0x1 \
             flexbytes (0x88,0x48) fwd pf queue 1 fd_id 1

flush_flow_director
~~~~~~~~~~~~~~~~~~~

Flush all flow director filters on a device::

   testpmd> flush_flow_director (port_id)

Example, to flush all flow director filter on port 0::

   testpmd> flush_flow_director 0

flow_director_mask
~~~~~~~~~~~~~~~~~~

Set flow director's input masks::

   flow_director_mask (port_id) mode IP vlan (vlan_value) \
                      src_mask (ipv4_src) (ipv6_src) (src_port) \
                      dst_mask (ipv4_dst) (ipv6_dst) (dst_port)

   flow_director_mask (port_id) mode MAC-VLAN vlan (vlan_value)

   flow_director_mask (port_id) mode Tunnel vlan (vlan_value) \
                      mac (mac_value) tunnel-type (tunnel_type_value) \
                      tunnel-id (tunnel_id_value)

Example, to set flow director mask on port 0::

   testpmd> flow_director_mask 0 mode IP vlan 0xefff \
            src_mask 255.255.255.255 \
                FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF 0xFFFF \
            dst_mask 255.255.255.255 \
                FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF 0xFFFF

flow_director_flex_mask
~~~~~~~~~~~~~~~~~~~~~~~

set masks of flow director's flexible payload based on certain flow type::

   testpmd> flow_director_flex_mask (port_id) \
            flow (none|ipv4-other|ipv4-frag|ipv4-tcp|ipv4-udp|ipv4-sctp| \
                  ipv6-other|ipv6-frag|ipv6-tcp|ipv6-udp|ipv6-sctp| \
                  l2_payload|all) (mask)

Example, to set flow director's flex mask for all flow type on port 0::

   testpmd> flow_director_flex_mask 0 flow all \
            (0xff,0xff,0,0,0,0,0,0,0,0,0,0,0,0,0,0)


flow_director_flex_payload
~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure flexible payload selection::

   flow_director_flex_payload (port_id) (raw|l2|l3|l4) (config)

For example, to select the first 16 bytes from the offset 4 (bytes) of packet's payload as flexible payload::

   testpmd> flow_director_flex_payload 0 l4 \
            (4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)

get_sym_hash_ena_per_port
~~~~~~~~~~~~~~~~~~~~~~~~~

Get symmetric hash enable configuration per port::

   get_sym_hash_ena_per_port (port_id)

For example, to get symmetric hash enable configuration of port 1::

   testpmd> get_sym_hash_ena_per_port 1

set_sym_hash_ena_per_port
~~~~~~~~~~~~~~~~~~~~~~~~~

Set symmetric hash enable configuration per port to enable or disable::

   set_sym_hash_ena_per_port (port_id) (enable|disable)

For example, to set symmetric hash enable configuration of port 1 to enable::

   testpmd> set_sym_hash_ena_per_port 1 enable

get_hash_global_config
~~~~~~~~~~~~~~~~~~~~~~

Get the global configurations of hash filters::

   get_hash_global_config (port_id)

For example, to get the global configurations of hash filters of port 1::

   testpmd> get_hash_global_config 1

set_hash_global_config
~~~~~~~~~~~~~~~~~~~~~~

Set the global configurations of hash filters::

   set_hash_global_config (port_id) (toeplitz|simple_xor|default) \
   (ipv4|ipv4-frag|ipv4-tcp|ipv4-udp|ipv4-sctp|ipv4-other|ipv6|ipv6-frag| \
   ipv6-tcp|ipv6-udp|ipv6-sctp|ipv6-other|l2_payload|<flow_id>) \
   (enable|disable)

For example, to enable simple_xor for flow type of ipv6 on port 2::

   testpmd> set_hash_global_config 2 simple_xor ipv6 enable

set_hash_input_set
~~~~~~~~~~~~~~~~~~

Set the input set for hash::

   set_hash_input_set (port_id) (ipv4-frag|ipv4-tcp|ipv4-udp|ipv4-sctp| \
   ipv4-other|ipv6-frag|ipv6-tcp|ipv6-udp|ipv6-sctp|ipv6-other| \
   l2_payload|<flow_id>) (ovlan|ivlan|src-ipv4|dst-ipv4|src-ipv6|dst-ipv6| \
   ipv4-tos|ipv4-proto|ipv6-tc|ipv6-next-header|udp-src-port|udp-dst-port| \
   tcp-src-port|tcp-dst-port|sctp-src-port|sctp-dst-port|sctp-veri-tag| \
   udp-key|gre-key|fld-1st|fld-2nd|fld-3rd|fld-4th|fld-5th|fld-6th|fld-7th| \
   fld-8th|none) (select|add)

For example, to add source IP to hash input set for flow type of ipv4-udp on port 0::

   testpmd> set_hash_input_set 0 ipv4-udp src-ipv4 add

set_fdir_input_set
~~~~~~~~~~~~~~~~~~

The Flow Director filters can match the different fields for different type of packet, i.e. specific input set
on per flow type and the flexible payload. This command can be used to change input set for each flow type.

Set the input set for flow director::

   set_fdir_input_set (port_id) (ipv4-frag|ipv4-tcp|ipv4-udp|ipv4-sctp| \
   ipv4-other|ipv6|ipv6-frag|ipv6-tcp|ipv6-udp|ipv6-sctp|ipv6-other| \
   l2_payload|<flow_id>) (ivlan|ethertype|src-ipv4|dst-ipv4|src-ipv6|dst-ipv6| \
   ipv4-tos|ipv4-proto|ipv4-ttl|ipv6-tc|ipv6-next-header|ipv6-hop-limits| \
   tudp-src-port|udp-dst-port|cp-src-port|tcp-dst-port|sctp-src-port| \
   sctp-dst-port|sctp-veri-tag|none) (select|add)

For example to add source IP to FD input set for flow type of ipv4-udp on port 0::

   testpmd> set_fdir_input_set 0 ipv4-udp src-ipv4 add

global_config
~~~~~~~~~~~~~

Set different GRE key length for input set::

   global_config (port_id) gre-key-len (number in bytes)

For example to set GRE key length for input set to 4 bytes on port 0::

   testpmd> global_config 0 gre-key-len 4


.. _testpmd_rte_flow:

Flow rules management
---------------------

Control of the generic flow API (*rte_flow*) is fully exposed through the
``flow`` command (validation, creation, destruction, queries and operation
modes).

Considering *rte_flow* overlaps with all `Filter Functions`_, using both
features simultaneously may cause undefined side-effects and is therefore
not recommended.

``flow`` syntax
~~~~~~~~~~~~~~~

Because the ``flow`` command uses dynamic tokens to handle the large number
of possible flow rules combinations, its behavior differs slightly from
other commands, in particular:

- Pressing *?* or the *<tab>* key displays contextual help for the current
  token, not that of the entire command.

- Optional and repeated parameters are supported (provided they are listed
  in the contextual help).

The first parameter stands for the operation mode. Possible operations and
their general syntax are described below. They are covered in detail in the
following sections.

- Check whether a flow rule can be created::

   flow validate {port_id}
       [group {group_id}] [priority {level}] [ingress] [egress]
       pattern {item} [/ {item} [...]] / end
       actions {action} [/ {action} [...]] / end

- Create a flow rule::

   flow create {port_id}
       [group {group_id}] [priority {level}] [ingress] [egress]
       pattern {item} [/ {item} [...]] / end
       actions {action} [/ {action} [...]] / end

- Destroy specific flow rules::

   flow destroy {port_id} rule {rule_id} [...]

- Destroy all flow rules::

   flow flush {port_id}

- Query an existing flow rule::

   flow query {port_id} {rule_id} {action}

- List existing flow rules sorted by priority, filtered by group
  identifiers::

   flow list {port_id} [group {group_id}] [...]

- Restrict ingress traffic to the defined flow rules::

   flow isolate {port_id} {boolean}

Validating flow rules
~~~~~~~~~~~~~~~~~~~~~

``flow validate`` reports whether a flow rule would be accepted by the
underlying device in its current state but stops short of creating it. It is
bound to ``rte_flow_validate()``::

   flow validate {port_id}
      [group {group_id}] [priority {level}] [ingress] [egress]
      pattern {item} [/ {item} [...]] / end
      actions {action} [/ {action} [...]] / end

If successful, it will show::

   Flow rule validated

Otherwise it will show an error message of the form::

   Caught error type [...] ([...]): [...]

This command uses the same parameters as ``flow create``, their format is
described in `Creating flow rules`_.

Check whether redirecting any Ethernet packet received on port 0 to RX queue
index 6 is supported::

   testpmd> flow validate 0 ingress pattern eth / end
      actions queue index 6 / end
   Flow rule validated
   testpmd>

Port 0 does not support TCPv6 rules::

   testpmd> flow validate 0 ingress pattern eth / ipv6 / tcp / end
      actions drop / end
   Caught error type 9 (specific pattern item): Invalid argument
   testpmd>

Creating flow rules
~~~~~~~~~~~~~~~~~~~

``flow create`` validates and creates the specified flow rule. It is bound
to ``rte_flow_create()``::

   flow create {port_id}
      [group {group_id}] [priority {level}] [ingress] [egress]
      pattern {item} [/ {item} [...]] / end
      actions {action} [/ {action} [...]] / end

If successful, it will return a flow rule ID usable with other commands::

   Flow rule #[...] created

Otherwise it will show an error message of the form::

   Caught error type [...] ([...]): [...]

Parameters describe in the following order:

- Attributes (*group*, *priority*, *ingress*, *egress* tokens).
- A matching pattern, starting with the *pattern* token and terminated by an
  *end* pattern item.
- Actions, starting with the *actions* token and terminated by an *end*
  action.

These translate directly to *rte_flow* objects provided as-is to the
underlying functions.

The shortest valid definition only comprises mandatory tokens::

   testpmd> flow create 0 pattern end actions end

Note that PMDs may refuse rules that essentially do nothing such as this
one.

**All unspecified object values are automatically initialized to 0.**

Attributes
^^^^^^^^^^

These tokens affect flow rule attributes (``struct rte_flow_attr``) and are
specified before the ``pattern`` token.

- ``group {group id}``: priority group.
- ``priority {level}``: priority level within group.
- ``ingress``: rule applies to ingress traffic.
- ``egress``: rule applies to egress traffic.

Each instance of an attribute specified several times overrides the previous
value as shown below (group 4 is used)::

   testpmd> flow create 0 group 42 group 24 group 4 [...]

Note that once enabled, ``ingress`` and ``egress`` cannot be disabled.

While not specifying a direction is an error, some rules may allow both
simultaneously.

Most rules affect RX therefore contain the ``ingress`` token::

   testpmd> flow create 0 ingress pattern [...]

Matching pattern
^^^^^^^^^^^^^^^^

A matching pattern starts after the ``pattern`` token. It is made of pattern
items and is terminated by a mandatory ``end`` item.

Items are named after their type (*RTE_FLOW_ITEM_TYPE_* from ``enum
rte_flow_item_type``).

The ``/`` token is used as a separator between pattern items as shown
below::

   testpmd> flow create 0 ingress pattern eth / ipv4 / udp / end [...]

Note that protocol items like these must be stacked from lowest to highest
layer to make sense. For instance, the following rule is either invalid or
unlikely to match any packet::

   testpmd> flow create 0 ingress pattern eth / udp / ipv4 / end [...]

More information on these restrictions can be found in the *rte_flow*
documentation.

Several items support additional specification structures, for example
``ipv4`` allows specifying source and destination addresses as follows::

   testpmd> flow create 0 ingress pattern eth / ipv4 src is 10.1.1.1
      dst is 10.2.0.0 / end [...]

This rule matches all IPv4 traffic with the specified properties.

In this example, ``src`` and ``dst`` are field names of the underlying
``struct rte_flow_item_ipv4`` object. All item properties can be specified
in a similar fashion.

The ``is`` token means that the subsequent value must be matched exactly,
and assigns ``spec`` and ``mask`` fields in ``struct rte_flow_item``
accordingly. Possible assignment tokens are:

- ``is``: match value perfectly (with full bit-mask).
- ``spec``: match value according to configured bit-mask.
- ``last``: specify upper bound to establish a range.
- ``mask``: specify bit-mask with relevant bits set to one.
- ``prefix``: generate bit-mask from a prefix length.

These yield identical results::

   ipv4 src is 10.1.1.1

::

   ipv4 src spec 10.1.1.1 src mask 255.255.255.255

::

   ipv4 src spec 10.1.1.1 src prefix 32

::

   ipv4 src is 10.1.1.1 src last 10.1.1.1 # range with a single value

::

   ipv4 src is 10.1.1.1 src last 0 # 0 disables range

Inclusive ranges can be defined with ``last``::

   ipv4 src is 10.1.1.1 src last 10.2.3.4 # 10.1.1.1 to 10.2.3.4

Note that ``mask`` affects both ``spec`` and ``last``::

   ipv4 src is 10.1.1.1 src last 10.2.3.4 src mask 255.255.0.0
      # matches 10.1.0.0 to 10.2.255.255

Properties can be modified multiple times::

   ipv4 src is 10.1.1.1 src is 10.1.2.3 src is 10.2.3.4 # matches 10.2.3.4

::

   ipv4 src is 10.1.1.1 src prefix 24 src prefix 16 # matches 10.1.0.0/16

Pattern items
^^^^^^^^^^^^^

This section lists supported pattern items and their attributes, if any.

- ``end``: end list of pattern items.

- ``void``: no-op pattern item.

- ``invert``: perform actions when pattern does not match.

- ``any``: match any protocol for the current layer.

  - ``num {unsigned}``: number of layers covered.

- ``pf``: match packets addressed to the physical function.

- ``vf``: match packets addressed to a virtual function ID.

  - ``id {unsigned}``: destination VF ID.

- ``port``: device-specific physical port index to use.

  - ``index {unsigned}``: physical port index.

- ``raw``: match an arbitrary byte string.

  - ``relative {boolean}``: look for pattern after the previous item.
  - ``search {boolean}``: search pattern from offset (see also limit).
  - ``offset {integer}``: absolute or relative offset for pattern.
  - ``limit {unsigned}``: search area limit for start of pattern.
  - ``pattern {string}``: byte string to look for.

- ``eth``: match Ethernet header.

  - ``dst {MAC-48}``: destination MAC.
  - ``src {MAC-48}``: source MAC.
  - ``type {unsigned}``: EtherType.

- ``vlan``: match 802.1Q/ad VLAN tag.

  - ``tpid {unsigned}``: tag protocol identifier.
  - ``tci {unsigned}``: tag control information.
  - ``pcp {unsigned}``: priority code point.
  - ``dei {unsigned}``: drop eligible indicator.
  - ``vid {unsigned}``: VLAN identifier.

- ``ipv4``: match IPv4 header.

  - ``tos {unsigned}``: type of service.
  - ``ttl {unsigned}``: time to live.
  - ``proto {unsigned}``: next protocol ID.
  - ``src {ipv4 address}``: source address.
  - ``dst {ipv4 address}``: destination address.

- ``ipv6``: match IPv6 header.

  - ``tc {unsigned}``: traffic class.
  - ``flow {unsigned}``: flow label.
  - ``proto {unsigned}``: protocol (next header).
  - ``hop {unsigned}``: hop limit.
  - ``src {ipv6 address}``: source address.
  - ``dst {ipv6 address}``: destination address.

- ``icmp``: match ICMP header.

  - ``type {unsigned}``: ICMP packet type.
  - ``code {unsigned}``: ICMP packet code.

- ``udp``: match UDP header.

  - ``src {unsigned}``: UDP source port.
  - ``dst {unsigned}``: UDP destination port.

- ``tcp``: match TCP header.

  - ``src {unsigned}``: TCP source port.
  - ``dst {unsigned}``: TCP destination port.

- ``sctp``: match SCTP header.

  - ``src {unsigned}``: SCTP source port.
  - ``dst {unsigned}``: SCTP destination port.
  - ``tag {unsigned}``: validation tag.
  - ``cksum {unsigned}``: checksum.

- ``vxlan``: match VXLAN header.

  - ``vni {unsigned}``: VXLAN identifier.

- ``e_tag``: match IEEE 802.1BR E-Tag header.

  - ``grp_ecid_b {unsigned}``: GRP and E-CID base.

- ``nvgre``: match NVGRE header.

  - ``tni {unsigned}``: virtual subnet ID.

- ``mpls``: match MPLS header.

  - ``label {unsigned}``: MPLS label.

- ``gre``: match GRE header.

  - ``protocol {unsigned}``: protocol type.

- ``fuzzy``: fuzzy pattern match, expect faster than default.

  - ``thresh {unsigned}``: accuracy threshold.

- ``gtp``, ``gtpc``, ``gtpu``: match GTPv1 header.

  - ``teid {unsigned}``: tunnel endpoint identifier.

- ``geneve``: match GENEVE header.

  - ``vni {unsigned}``: virtual network identifier.
  - ``protocol {unsigned}``: protocol type.

Actions list
^^^^^^^^^^^^

A list of actions starts after the ``actions`` token in the same fashion as
`Matching pattern`_; actions are separated by ``/`` tokens and the list is
terminated by a mandatory ``end`` action.

Actions are named after their type (*RTE_FLOW_ACTION_TYPE_* from ``enum
rte_flow_action_type``).

Dropping all incoming UDPv4 packets can be expressed as follows::

   testpmd> flow create 0 ingress pattern eth / ipv4 / udp / end
      actions drop / end

Several actions have configurable properties which must be specified when
there is no valid default value. For example, ``queue`` requires a target
queue index.

This rule redirects incoming UDPv4 traffic to queue index 6::

   testpmd> flow create 0 ingress pattern eth / ipv4 / udp / end
      actions queue index 6 / end

While this one could be rejected by PMDs (unspecified queue index)::

   testpmd> flow create 0 ingress pattern eth / ipv4 / udp / end
      actions queue / end

As defined by *rte_flow*, the list is not ordered, all actions of a given
rule are performed simultaneously. These are equivalent::

   queue index 6 / void / mark id 42 / end

::

   void / mark id 42 / queue index 6 / end

All actions in a list should have different types, otherwise only the last
action of a given type is taken into account::

   queue index 4 / queue index 5 / queue index 6 / end # will use queue 6

::

   drop / drop / drop / end # drop is performed only once

::

   mark id 42 / queue index 3 / mark id 24 / end # mark will be 24

Considering they are performed simultaneously, opposite and overlapping
actions can sometimes be combined when the end result is unambiguous::

   drop / queue index 6 / end # drop has no effect

::

   drop / dup index 6 / end # same as above

::

   queue index 6 / rss queues 6 7 8 / end # queue has no effect

::

   drop / passthru / end # drop has no effect

Note that PMDs may still refuse such combinations.

Actions
^^^^^^^

This section lists supported actions and their attributes, if any.

- ``end``: end list of actions.

- ``void``: no-op action.

- ``passthru``: let subsequent rule process matched packets.

- ``mark``: attach 32 bit value to packets.

  - ``id {unsigned}``: 32 bit value to return with packets.

- ``flag``: flag packets.

- ``queue``: assign packets to a given queue index.

  - ``index {unsigned}``: queue index to use.

- ``drop``: drop packets (note: passthru has priority).

- ``count``: enable counters for this rule.

- ``dup``: duplicate packets to a given queue index.

  - ``index {unsigned}``: queue index to duplicate packets to.

- ``rss``: spread packets among several queues.

  - ``queues [{unsigned} [...]] end``: queue indices to use.

- ``pf``: redirect packets to physical device function.

- ``vf``: redirect packets to virtual device function.

  - ``original {boolean}``: use original VF ID if possible.
  - ``id {unsigned}``: VF ID to redirect packets to.

Destroying flow rules
~~~~~~~~~~~~~~~~~~~~~

``flow destroy`` destroys one or more rules from their rule ID (as returned
by ``flow create``), this command calls ``rte_flow_destroy()`` as many
times as necessary::

   flow destroy {port_id} rule {rule_id} [...]

If successful, it will show::

   Flow rule #[...] destroyed

It does not report anything for rule IDs that do not exist. The usual error
message is shown when a rule cannot be destroyed::

   Caught error type [...] ([...]): [...]

``flow flush`` destroys all rules on a device and does not take extra
arguments. It is bound to ``rte_flow_flush()``::

   flow flush {port_id}

Any errors are reported as above.

Creating several rules and destroying them::

   testpmd> flow create 0 ingress pattern eth / ipv6 / end
      actions queue index 2 / end
   Flow rule #0 created
   testpmd> flow create 0 ingress pattern eth / ipv4 / end
      actions queue index 3 / end
   Flow rule #1 created
   testpmd> flow destroy 0 rule 0 rule 1
   Flow rule #1 destroyed
   Flow rule #0 destroyed
   testpmd>

The same result can be achieved using ``flow flush``::

   testpmd> flow create 0 ingress pattern eth / ipv6 / end
      actions queue index 2 / end
   Flow rule #0 created
   testpmd> flow create 0 ingress pattern eth / ipv4 / end
      actions queue index 3 / end
   Flow rule #1 created
   testpmd> flow flush 0
   testpmd>

Non-existent rule IDs are ignored::

   testpmd> flow create 0 ingress pattern eth / ipv6 / end
      actions queue index 2 / end
   Flow rule #0 created
   testpmd> flow create 0 ingress pattern eth / ipv4 / end
      actions queue index 3 / end
   Flow rule #1 created
   testpmd> flow destroy 0 rule 42 rule 10 rule 2
   testpmd>
   testpmd> flow destroy 0 rule 0
   Flow rule #0 destroyed
   testpmd>

Querying flow rules
~~~~~~~~~~~~~~~~~~~

``flow query`` queries a specific action of a flow rule having that
ability. Such actions collect information that can be reported using this
command. It is bound to ``rte_flow_query()``::

   flow query {port_id} {rule_id} {action}

If successful, it will display either the retrieved data for known actions
or the following message::

   Cannot display result for action type [...] ([...])

Otherwise, it will complain either that the rule does not exist or that some
error occurred::

   Flow rule #[...] not found

::

   Caught error type [...] ([...]): [...]

Currently only the ``count`` action is supported. This action reports the
number of packets that hit the flow rule and the total number of bytes. Its
output has the following format::

   count:
    hits_set: [...] # whether "hits" contains a valid value
    bytes_set: [...] # whether "bytes" contains a valid value
    hits: [...] # number of packets
    bytes: [...] # number of bytes

Querying counters for TCPv6 packets redirected to queue 6::

   testpmd> flow create 0 ingress pattern eth / ipv6 / tcp / end
      actions queue index 6 / count / end
   Flow rule #4 created
   testpmd> flow query 0 4 count
   count:
    hits_set: 1
    bytes_set: 0
    hits: 386446
    bytes: 0
   testpmd>

Listing flow rules
~~~~~~~~~~~~~~~~~~

``flow list`` lists existing flow rules sorted by priority and optionally
filtered by group identifiers::

   flow list {port_id} [group {group_id}] [...]

This command only fails with the following message if the device does not
exist::

   Invalid port [...]

Output consists of a header line followed by a short description of each
flow rule, one per line. There is no output at all when no flow rules are
configured on the device::

   ID      Group   Prio    Attr    Rule
   [...]   [...]   [...]   [...]   [...]

``Attr`` column flags:

- ``i`` for ``ingress``.
- ``e`` for ``egress``.

Creating several flow rules and listing them::

   testpmd> flow create 0 ingress pattern eth / ipv4 / end
      actions queue index 6 / end
   Flow rule #0 created
   testpmd> flow create 0 ingress pattern eth / ipv6 / end
      actions queue index 2 / end
   Flow rule #1 created
   testpmd> flow create 0 priority 5 ingress pattern eth / ipv4 / udp / end
      actions rss queues 6 7 8 end / end
   Flow rule #2 created
   testpmd> flow list 0
   ID      Group   Prio    Attr    Rule
   0       0       0       i-      ETH IPV4 => QUEUE
   1       0       0       i-      ETH IPV6 => QUEUE
   2       0       5       i-      ETH IPV4 UDP => RSS
   testpmd>

Rules are sorted by priority (i.e. group ID first, then priority level)::

   testpmd> flow list 1
   ID      Group   Prio    Attr    Rule
   0       0       0       i-      ETH => COUNT
   6       0       500     i-      ETH IPV6 TCP => DROP COUNT
   5       0       1000    i-      ETH IPV6 ICMP => QUEUE
   1       24      0       i-      ETH IPV4 UDP => QUEUE
   4       24      10      i-      ETH IPV4 TCP => DROP
   3       24      20      i-      ETH IPV4 => DROP
   2       24      42      i-      ETH IPV4 UDP => QUEUE
   7       63      0       i-      ETH IPV6 UDP VXLAN => MARK QUEUE
   testpmd>

Output can be limited to specific groups::

   testpmd> flow list 1 group 0 group 63
   ID      Group   Prio    Attr    Rule
   0       0       0       i-      ETH => COUNT
   6       0       500     i-      ETH IPV6 TCP => DROP COUNT
   5       0       1000    i-      ETH IPV6 ICMP => QUEUE
   7       63      0       i-      ETH IPV6 UDP VXLAN => MARK QUEUE
   testpmd>

Toggling isolated mode
~~~~~~~~~~~~~~~~~~~~~~

``flow isolate`` can be used to tell the underlying PMD that ingress traffic
must only be injected from the defined flow rules; that no default traffic
is expected outside those rules and the driver is free to assign more
resources to handle them. It is bound to ``rte_flow_isolate()``::

 flow isolate {port_id} {boolean}

If successful, enabling or disabling isolated mode shows either::

 Ingress traffic on port [...]
    is now restricted to the defined flow rules

Or::

 Ingress traffic on port [...]
    is not restricted anymore to the defined flow rules

Otherwise, in case of error::

   Caught error type [...] ([...]): [...]

Mainly due to its side effects, PMDs supporting this mode may not have the
ability to toggle it more than once without reinitializing affected ports
first (e.g. by exiting testpmd).

Enabling isolated mode::

 testpmd> flow isolate 0 true
 Ingress traffic on port 0 is now restricted to the defined flow rules
 testpmd>

Disabling isolated mode::

 testpmd> flow isolate 0 false
 Ingress traffic on port 0 is not restricted anymore to the defined flow rules
 testpmd>

Sample QinQ flow rules
~~~~~~~~~~~~~~~~~~~~~~

Before creating QinQ rule(s) the following commands should be issued to enable QinQ::

   testpmd> port stop 0
   testpmd> vlan set qinq on 0

The above command sets the inner and outer TPID's to 0x8100.

To change the TPID's the following commands should be used::

   testpmd> vlan set outer tpid 0xa100 0
   testpmd> vlan set inner tpid 0x9100 0
   testpmd> port start 0

Validate and create a QinQ rule on port 0 to steer traffic to a VF queue in a VM.

::

   testpmd> flow validate 0 ingress pattern eth / vlan tci is 123 /
       vlan tci is 456 / end actions vf id 1 / queue index 0 / end
   Flow rule #0 validated

   testpmd> flow create 0 ingress pattern eth / vlan tci is 4 /
       vlan tci is 456 / end actions vf id 123 / queue index 0 / end
   Flow rule #0 created

   testpmd> flow list 0
   ID      Group   Prio    Attr    Rule
   0       0       0       i-      ETH VLAN VLAN=>VF QUEUE

Validate and create a QinQ rule on port 0 to steer traffic to a queue on the host.

::

   testpmd> flow validate 0 ingress pattern eth / vlan tci is 321 /
        vlan tci is 654 / end actions pf / queue index 0 / end
   Flow rule #1 validated

   testpmd> flow create 0 ingress pattern eth / vlan tci is 321 /
        vlan tci is 654 / end actions pf / queue index 1 / end
   Flow rule #1 created

   testpmd> flow list 0
   ID      Group   Prio    Attr    Rule
   0       0       0       i-      ETH VLAN VLAN=>VF QUEUE
   1       0       0       i-      ETH VLAN VLAN=>PF QUEUE
