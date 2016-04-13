# This program was created by Rafael Rocha de Carvalho for the class of Distributed Data Management taught at UFPR by Professor Eduardo Almeida. 
# This code is intended to implement the DHT using a test.in input file and writing the output to file test.out

require 'pry'

class Operations
    
    attr_accessor :operationId, :operation, :operationElement, :operationElement2 
    
    def initialize(id, op, opElement, opElement2)  
    # Instance variables  
        @operationId = id
        @operation = op 
        @operationElement = opElement
        @operationElement2 = opElement2
    end
end

$nNodes = 0
$endNode = 0
$startNode = nil
$data = Array.new # array that receive the data from input file
$output = Array.new # array that receive the output to write
$route = Array.new # array that receive the route
$opHashKeys =  Hash.new
$opHashRoute =  Hash.new



#TODO: Read from stdin
def readFile
    File.open(ARGV[0], "r") do |f|
      f.each_line do |line|
        if line != " " or line != ""
            auxData = line.split
            n = Operations.new(auxData[0],auxData[1],auxData[2],auxData[3])
            $data.push(n)
        end
      end
    end
end

# create the keys that an node will guard
def createKeys(keyvalue)
    keys = Array.new
    max = keyvalue - 1
    max.downto(0) { 
        |i| 
        if($opHashKeys[i] == nil || $opHashKeys[i] == "")
            keys.push(i)
        else
            break
        end
    }
    return keys
end

def updateTable (keyValue)
    for i in (keyValue + 1)  .. $endNode
        if($opHashKeys[i] != nil && $opHashKeys[i] != "")
            $opHashKeys[i] = createKeys(i)
            break
        end
    end
end

#Cria tabela de rotas
def createRouteTable(index)
    logValue = Math::log($nNodes, 2)
    logValue = logValue.floor
    i = 0
    auxIndex = index
    if(auxIndex == $endNode)
        auxIndex = -1
    end
    nodeOption = auxIndex + (2**i) 
    arrayRoutes = Array.new
    while ($nNodes > 1 && arrayRoutes.size < logValue) do
        for indexOfHash in nodeOption .. $endNode
            if($opHashKeys[indexOfHash] != nil && $opHashKeys[indexOfHash] != "" )
                arrayRoutes.push(indexOfHash)
                arrayRoutes = arrayRoutes.uniq
                break
            end
        end
        i+=1
        nodeOption = auxIndex + (2**i) 
        if(nodeOption > $endNode)
           auxIndex = -1
           nodeOption = auxIndex + (2**i) 
        end
    end
    return arrayRoutes.sort
end

def updateRouteTable (keyValue)
    max = keyValue - 1
    max.downto($startNode){
        |i|
        if($opHashRoute[i] != nil && $opHashRoute[i] != "")
            $opHashRoute[i] = createRouteTable(i)
        end
    }
end

# Put value in hash and update table
def createHash(operation)
    if(Integer(operation.operationElement) > $endNode)
        $endNode = Integer(operation.operationElement)
    end
    if($startNode == nil || Integer(operation.operationElement) < $startNode)
        $startNode = Integer(operation.operationElement)
    end
    $opHashKeys[Integer(operation.operationElement)] = createKeys(Integer(operation.operationElement))
    $opHashRoute[Integer(operation.operationElement)] = createRouteTable(Integer(operation.operationElement))
    
    updateTable(Integer(operation.operationElement))
    updateRouteTable(Integer(operation.operationElement))
end

# Remove value in hash and update table
def removeHash(operation)
    keysArray = $opHashKeys[Integer(operation.operationElement)]
    starter = Integer(operation.operationElement) + 1
    if(starter == $endNode)
        for i in  ($endNode - 1) .. 0
            if($opHashKeys[i] != nil && $opHashKeys[i] != "")
                $endNode = i
                break
            end
        end
    else
        for i in  starter .. $endNode
            if($opHashKeys[i] != nil && $opHashKeys[i] != "")
                auxArray = $opHashKeys[i]
                auxArray.concat keysArray
                auxArray.sort! {|x, y| y <=> x}
                $opHashKeys[i] = auxArray
                if($startNode == starter)
                    $startNode = i
                end
                break
            end
        end
    end
    
    $opHashKeys.delete(Integer(operation.operationElement))
    $opHashRoute.delete(Integer(operation.operationElement))
    
    updateRouteTable(Integer(operation.operationElement))
end

def lookup(nodeRoute, id, findValue, firstElement)
    keysArray = $opHashKeys[nodeRoute]
    p keysArray
    if keysArray.include?(findValue)
        outputString = "#{id} L #{firstElement} {#{$route.join(",")}}"
        $output.push(outputString)
    else
        routes = $opHashRoute[nodeRoute]
        bvalue = 0
        routes.each { 
            |routeValue|
                bvalue = routeValue
                if(routeValue > findValue && routeValue != nodeRoute)
                    puts routeValue
                    $route.push(routeValue)
                    lookup(routeValue, id, findValue, firstElement)
                    bvalue = 0
                    break
                end
        }
        if(bvalue != 0)
            $route.push(bvalue)
            lookup(bvalue, id, findValue, firstElement)
        end
    end 
end

# Method that test the data received from input file
def testDataReceived
    #p $data
    $data.each { 
        |operation|
        puts "#{operation.operationId} #{operation.operation} #{operation.operationElement} #{operation.operationElement2}"
        if(operation.operation == "E")
             $nNodes+=1
            createHash(operation)
            $opHashKeys = $opHashKeys.sort.to_h
            $opHashRoute = $opHashRoute.sort.to_h
        elsif(operation.operation == "S")
            $nNodes-=1
            removeHash(operation)
            $opHashKeys = $opHashKeys.sort.to_h
            $opHashRoute = $opHashRoute.sort.to_h
        elsif(operation.operation == "L")
            puts "Cheguei lookup"
            lookup(Integer(operation.operationElement),operation.operationId,Integer(operation.operationElement2), operation.operationElement)
            $route = Array.new
        end
    }
end

readFile
testDataReceived

puts "--------------"
puts "Numero de nos #{$nNodes}"

puts $opHashKeys
puts $opHashRoute
p $output