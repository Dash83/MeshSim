
#Current Problems

##Network operations fail some times.
####Possible Solutions
1. Change network library
2. Keep debugging the code until a fix is found.

#####Notes
On solution 1:

* Would also fix cross-compile issues.
* No guarantee it will fix things.
* Might be too late/difficult to do.

On solution 2

* Frustrating, but should be effective.
* I don't know where to start. The calls my code makes are succesful.

##Cross compilation fails.
####Possible Solutions
1. Remove nanomsg dependency
2. Install nanomsg cross-compiled library
3. Compile on the RPI every time.
 
#Fixed Problems
~~##Can't developt on my Mac
    *This is a major problem, since I won't finish here and I need to keep coding while on the trip.
    ###Possible Solutions
    *Change avahi for bonjour when on Mac (horrible idea, but possible).
    *Setup VM to compile and run examples on Mac.
~~

#Missing features for demo
1. Fully functioning network capabilities.
2. Coding the protocol that will be displayed.
3. Outputting data I can show.


#Current Plan
1. Implemente a full membership protocol to show. (addresses missing feature 2).
2. Stick with Nanomsg. Debug using socat and google for debugging UDS.
3. Configure on RPI to work with my github account for pulling and compiling.
4. Test 2 devices in device mode. 
5. Possible solution for problem 1: mio-uds.

#Resources
##DNS & DNS-SD
* [DNS TXT Records](https://en.wikipedia.org/wiki/TXT_record)
* [DNS SRV](https://tools.ietf.org/html/rfc2782)
* [SRV Records](https://en.wikipedia.org/wiki/SRV_record)
* [mDNS](https://tools.ietf.org/html/rfc6762) 
* https://en.wikipedia.org/wiki/Multicast_DNS
* [DNS SD](https://tools.ietf.org/html/rfc6763)

##Zero-Conf Networking
* https://en.wikipedia.org/wiki/Zero-configuration_networking

##Raspberry Pi + Rust
* https://hackernoon.com/compiling-rust-for-the-raspberry-pi-49fdcd7df658
* https://stackoverflow.com/questions/37375712/cross-compile-rust-openssl-for-raspberry-pi-2?rq=1

##Socket programming
* [Network Programming tutorial](http://www.tenouk.com/Module39.html)
* [Beej's guide to network programming](https://beej.us/guide/bgnet/output/html/multipage/index.html) (Me quede en la seccion 6)
* https://nbaksalyar.github.io/2015/07/10/writing-chat-in-rust.html

##Avahi
* https://serverfault.com/questions/118237/how-to-use-zeroconf
* https://superuser.com/questions/1236425/get-static-ip-to-be-alias-for-avahi-resolved-dynamic-ip-for-sane-scanner
* http://avahi.org/
* [Avahi Documentation](https://www.avahi.org/doxygen/html/index.html)

##TCP dump
* https://danielmiessler.com/study/tcpdump/

##Rust network programming
* [Low level network programming](https://github.com/libpnet/libpnet)

##Linux wireless subsystem
* [man iwconfig](https://manpages.debian.org/stretch/wireless-tools/iwconfig.8.en.html)
* [How to get wireless driver](http://ask.xmodulo.com/network-card-driver-name-version-linux.html)

##Manet & Ad-Hoc
* https://www.kwalinux.nl/what-a-mesh/1129/
* http://www.manet-routing.org/
* https://serverfault.com/questions/102766/create-a-manet-mobile-ad-hoc-network-with-gnu-linux-debian-based-hosts
