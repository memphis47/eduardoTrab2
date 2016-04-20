# This program was created by Rafael Rocha de Carvalho for the class of Distributed Data Management taught at UFPR by Professor Eduardo Almeida. 
# This code is intended to implement the DHT using a test.in input file and writing the output to file test.out
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
    STDIN.read.split("\n").each do |line|
         if line != " " or line != ""
            auxData = line.split
            n = Operations.new(auxData[0],auxData[1],auxData[2],auxData[3])
            $data.push(n)
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

def updateTable(keyValue)
    if(keyValue == $endNode)
        updateTableArray($startNode,keyValue)
    else
        for i in (keyValue + 1) .. $endNode
            if($opHashKeys[i] != nil && $opHashKeys[i] != "")
                updateTableArray(i,keyValue)
                break
            end 
        end
    end
end

def updateTableArray(i,keyvalue)
    keysArray = $opHashKeys[i]
    if(!keysArray.empty?)
        newArray = keysArray.select{ |a| a > i && a <= keyvalue }
        keysArray.delete_if{ |a| a > i && a <= keyvalue } 
        $opHashKeys[i] = keysArray
        $opHashKeys[keyvalue] = newArray
    end
end

#def updateTable (keyValue)
#    for i in (keyValue + 1)  .. $endNode
#        if($opHashKeys[i] != nil && $opHashKeys[i] != "")
#            $opHashKeys[i] = createKeys(i)
#            break
#        end
#    end
#end

#Cria tabela de rotas
def createRouteTable(index)
    logValue = Math::log($nNodes, 2)
    logValue = logValue.floor
    i = 0
    auxIndex = index
    #if(auxIndex == $endNode)
    #    auxIndex = -1
   # end
    mValue = $endNode.to_s(2).length
    nodeOption = (auxIndex + (2**i)) % (2**mValue)
    arrayRoutes = Array.new
    while ($nNodes > 1 && arrayRoutes.size < logValue) do
        for indexOfHash in nodeOption .. $endNode
            if($opHashKeys[indexOfHash] != nil && $opHashKeys[indexOfHash] != "")
                arrayRoutes.push(indexOfHash)
                arrayRoutes = arrayRoutes.uniq
                break
            end
        end
        i+=1
        nodeOption = (auxIndex + (2**i)) % (2**mValue)
    end
    return arrayRoutes.sort
end

def updateRouteTable
    for i in $startNode .. $endNode
        if($opHashRoute[i] != nil && $opHashRoute[i] != "")
            $opHashRoute[i] = createRouteTable(i)
        end
    end
end

# Put value in hash and update table
def createHash(operation)
    if(Integer(operation.operationElement) > $endNode)
        $endNode = Integer(operation.operationElement)
    end
    if($startNode == nil || Integer(operation.operationElement) < $startNode)
        $startNode = Integer(operation.operationElement)
    end
    #$opHashKeys[Integer(operation.operationElement)] = createKeys(Integer(operation.operationElement))
    $opHashKeys[Integer(operation.operationElement)] = Array.new
    $opHashRoute[Integer(operation.operationElement)] = createRouteTable(Integer(operation.operationElement))
    updateTable(Integer(operation.operationElement))
    updateRouteTable
end

def updateEndNode
    max = $endNode - 1 
    max.downto($startNode){
        |i|
        if($opHashKeys[i] != nil && $opHashKeys[i] != "")
            $endNode = i
            break
        end
    }
end

# Remove value in hash and update table
def removeHash(operation)
    keysArray = $opHashKeys[Integer(operation.operationElement)]
    routeArray =  $opHashRoute[Integer(operation.operationElement)]
    
    starter = Integer(operation.operationElement)
    if(starter == $endNode)
        for i in  ($endNode - 1) .. 0
            if($opHashKeys[i] != nil && $opHashKeys[i] != "")
                $endNode = i
                break
            end
        end
        
        auxArray = $opHashKeys[$startNode]
        auxArray.concat keysArray
        auxArray.sort! {|x, y| y <=> x}
        $opHashKeys[$startNode] = auxArray
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
    
    if($startNode == Integer(operation.operationElement))
        $startNode = routeArray.first
    elsif $endNode == Integer(operation.operationElement)
        updateEndNode
    end
    
    updateRouteTable
end

def lookup(nodeRoute, id, findValue, firstElement)
    keysArray = $opHashKeys[nodeRoute]
    if keysArray.include?(findValue)
        outputString = "#{id} L #{findValue} {#{$route.join(",")}}"
        $output.push(outputString)
    else
        routes = $opHashRoute[nodeRoute]
        bvalue = 0
        routes.each { 
            |routeValue|
                bvalue = routeValue
                if(routeValue > findValue && routeValue != nodeRoute)
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

def insert(insertKey)
    if(insertKey > $endNode)
        keysArray =  $opHashKeys[$startNode]
        keysArray.push(insertKey)
    else
        for i in insertKey .. $endNode
            if($opHashKeys[i] != nil && $opHashKeys[i] != "")
                keysArray = $opHashKeys[i]
                keysArray.push(insertKey)
                $opHashKeys[i] = keysArray
                break
            end
        end
    end
        
end

# Method that test the data received from input file
def testDataReceived
    #p $data
    $data.each { 
        |operation|
        if(operation.operation == "E")
             $nNodes+=1
            createHash(operation)
        elsif(operation.operation == "S")
            $nNodes-=1
            removeHash(operation)
        elsif(operation.operation == "I")
            insert(Integer(operation.operationElement2))
            $route = Array.new
        elsif(operation.operation == "L")
            lookup(Integer(operation.operationElement),operation.operationId,Integer(operation.operationElement2), operation.operationElement)
            writeFile(operation.operationId)
            $route = Array.new
            $output = Array.new
            $tOutput = Array.new
        end
        
    }
end

def writeFile(id)
    $output.each {
        |result|
        puts result
    }
    keys = $opHashRoute.keys 
    keys = keys.sort
    keys.each {
        |key|
        routes = $opHashRoute[key]
        puts "#{id} T #{key} {#{routes.join(",")}}"
    }
end

readFile
testDataReceived