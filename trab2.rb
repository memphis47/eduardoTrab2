# This program was created by Rafael Rocha de Carvalho for the class of Distributed Data Management taught at UFPR by Professor Eduardo Almeida. 
# This code is intended to implement the DHT using a test.in input file and writing the output to file test.out

class Operations
    
    attr_accessor :operationId, :operation, :operationElement, :operationElement2 
    
    def initialize(op, opElement, opElement2)  
    # Instance variables  
        @operation = op 
        @operationElement = opElement
        @operationElement2 = opElement2
    end
end

$nNodes = 0
$endNode = 0
$startNode
$data = Array.new # array that receive the data from input file
$opHashKeys =  Hash.new
$opHashRoute =  Hash.new


#TODO: Read from stdin
def readFile
    File.open(ARGV[0], "r") do |f|
      f.each_line do |line|
        if line != " " or line != ""
            auxData = line.split
            n = Operations.new(auxData[1],auxData[2],auxData[3])
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
    puts "Log : #{logValue}"
    i = 0
    auxIndex = index
    if(auxIndex == $endNode)
        auxIndex = -1
    end
    nodeOption = index + (2**i) 
    arrayRoutes = Array.new
    while (nodeOption <= $endNode && arrayRoutes.size < logValue) do
        puts ("Array Size: #{arrayRoutes.size}")
        for indexOfHash in nodeOption .. $endNode
            if($opHashKeys[indexOfHash] != nil && $opHashKeys[indexOfHash] != "" && indexOfHash != index)
                arrayRoutes.push(indexOfHash)
                arrayRoutes = arrayRoutes.uniq
                break
            end
        end
        i+=1
        nodeOption = auxIndex + (2**i) 
    end
    return arrayRoutes
end

def updateRouteTable (keyValue)
    max = keyValue - 1
    max.downto($startNode){
        |i|
        if($opHashRoute[i] != nil && $opHashRoute[i] != "")
            $opHashRoute[i] = createRouteTable(i)
            break
        end
    }
end

# Put value in hash and update table
def createHash(operation)
    $opHashKeys[Integer(operation.operationElement)] = createKeys(Integer(operation.operationElement))
    $opHashRoute[Integer(operation.operationElement)] = createRouteTable(Integer(operation.operationElement))
    if(Integer(operation.operationElement) > $endNode)
        $endNode = Integer(operation.operationElement)
    end
    if($startNode == nil || Integer(operation.operationElement) < $startNode)
        $startNode = Integer(operation.operationElement)
    end
    
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


# Method that test the data received from input file
def testDataReceived
    #p $data
    $data.each { 
        |operation|
        puts operation.operation
        puts operation.operationElement
        if(operation.operation == "E")
             $nNodes+=1
            createHash(operation)
           
        elsif(operation.operation == "S")
            $nNodes-=1
            removeHash(operation)
        end   
    }
end

readFile
testDataReceived

puts "--------------"
puts $nNodes
puts $opHashKeys
puts $opHashRoute